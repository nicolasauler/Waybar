#include "modules/hyprland/backend.hpp"

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

#include "util/scoped_fd.hpp"
#include "util/string.hpp"

namespace waybar::modules::hyprland {

namespace {
// Hyprland drops a socket2 client whose event queue overflows: one that stops reading for a few
// seconds while events arrive quickly. Reconnect right away after a connection that was up for a
// while; back off when connecting fails or connections keep closing as soon as they open.
constexpr std::chrono::milliseconds kRetryDelayMin{100};
constexpr std::chrono::milliseconds kRetryDelayMax{5000};
constexpr std::chrono::seconds kStableConnection{1};

// An inactivity timeout for each socket1 operation (connecting, writing, each read): a Hyprland
// that stops answering fails the request instead of blocking a module thread, or the listener's
// shutdown, forever. It is not a deadline for a whole request, nor for shutdown.
constexpr timeval kRequestTimeout{.tv_sec = 5, .tv_usec = 0};

// Returns the connected socket, or -1 with errno set. Connecting does not block: with Hyprland's
// accept backlog full it fails with EAGAIN, and the caller retries after an interruptible delay.
int connectEventSocket(const std::filesystem::path& socketPath) {
  const int socketfd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
  if (socketfd == -1) {
    return -1;
  }

  sockaddr_un addr = {};
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, socketPath.c_str(), sizeof(addr.sun_path) - 1);

  const auto fail = [socketfd] {
    const int error = errno;
    close(socketfd);
    errno = error;
    return -1;
  };
  if (connect(socketfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == -1) {
    return fail();
  }
  // Reading blocks; shutdown() from the destructor ends it.
  const int flags = fcntl(socketfd, F_GETFL);
  if (flags == -1 || fcntl(socketfd, F_SETFL, flags & ~O_NONBLOCK) == -1) {
    return fail();
  }
  return socketfd;
}
}  // namespace
std::filesystem::path IPC::socketFolder_;
std::optional<bool> IPC::s_luaProtocolDetected_;

std::filesystem::path IPC::getSocketFolder(const char* instanceSig) {
  static std::mutex folderMutex;
  std::unique_lock lock(folderMutex);

  if (socketFolder_.empty()) {
    const char* xdgRuntimeDirEnv = std::getenv("XDG_RUNTIME_DIR");
    std::filesystem::path xdgRuntimeDir;
    // Only set path if env variable is set
    if (xdgRuntimeDirEnv != nullptr) {
      xdgRuntimeDir = std::filesystem::path(xdgRuntimeDirEnv);
    }

    if (!xdgRuntimeDir.empty() && std::filesystem::exists(xdgRuntimeDir / "hypr")) {
      socketFolder_ = xdgRuntimeDir / "hypr";
    } else {
      spdlog::warn("$XDG_RUNTIME_DIR/hypr does not exist, falling back to /tmp/hypr");
      socketFolder_ = std::filesystem::path("/tmp") / "hypr";
    }
  }

  return socketFolder_ / instanceSig;
}

IPC::IPC() {
  // will start IPC and relay events to parseIPC
  socketOwnerPid_ = getpid();
  ipcThread_ = std::thread([this]() { socketListener(); });
}

IPC::~IPC() {
  // Do no stop Hyprland IPC if a child process (with successful fork() but
  // failed exec()) exits.
  if (getpid() != socketOwnerPid_) return;

  running_.store(false, std::memory_order_relaxed);
  spdlog::info("Hyprland IPC stopping...");
  {
    std::lock_guard<std::mutex> lock(socketMutex_);
    if (socketfd_ != -1) {
      spdlog::trace("Shutting down socket");
      if (shutdown(socketfd_, SHUT_RDWR) == -1 && errno != ENOTCONN) {
        spdlog::error("Hyprland IPC: Couldn't shutdown socket");
      }
    }
  }
  stopRequested_.notify_all();
  if (ipcThread_.joinable()) {
    ipcThread_.join();
  }
}

IPC& IPC::inst() {
  static IPC ipc;
  return ipc;
}

void IPC::socketListener() {
  // check for hyprland
  const char* his = getenv("HYPRLAND_INSTANCE_SIGNATURE");

  if (his == nullptr) {
    spdlog::warn("Hyprland is not running, Hyprland IPC will not be available.");
    return;
  }

  spdlog::info("Hyprland IPC starting");

  const auto socketPath = IPC::getSocketFolder(his) / ".socket2.sock";
  if (socketPath.native().size() >= sizeof(sockaddr_un::sun_path)) {
    spdlog::error("Hyprland IPC: Socket path is too long: {}", socketPath.string());
    return;
  }
  auto retryDelay = kRetryDelayMin;
  bool missedEvents = false;  // Hyprland may have sent events while we were not connected
  bool failureLogged = false;

  while (running_.load(std::memory_order_relaxed)) {
    const int socketfd = connectEventSocket(socketPath);
    if (socketfd == -1) {
      if (!failureLogged) {
        spdlog::error("Hyprland IPC: Unable to connect? {}", std::strerror(errno));
        failureLogged = true;
      }
      missedEvents = true;
      if (!waitBeforeRetry(retryDelay)) {
        break;
      }
      retryDelay = std::min(retryDelay * 2, kRetryDelayMax);
      continue;
    }
    failureLogged = false;

    {
      std::lock_guard<std::mutex> lock(socketMutex_);
      if (!running_.load(std::memory_order_relaxed)) {
        close(socketfd);
        break;
      }
      socketfd_ = socketfd;
    }

    if (missedEvents) {
      spdlog::info("Hyprland IPC connected after missing events; refreshing modules");
      refreshAfterReconnect();
    }

    const auto connectedAt = std::chrono::steady_clock::now();
    readEvents(socketfd);
    closeEventSocket();
    missedEvents = true;

    if (std::chrono::steady_clock::now() - connectedAt >= kStableConnection) {
      retryDelay = kRetryDelayMin;
    } else if (running_.load(std::memory_order_relaxed)) {
      if (!waitBeforeRetry(retryDelay)) {
        break;
      }
      retryDelay = std::min(retryDelay * 2, kRetryDelayMax);
    }
  }
  spdlog::debug("Hyprland IPC stopped");
}

void IPC::readEvents(int socketfd) {
  std::string pending;
  while (running_.load(std::memory_order_relaxed)) {
    std::array<char, 1024> buffer;  // Hyprland socket2 events are max 1024 bytes
    const ssize_t bytes_read = read(socketfd, buffer.data(), buffer.size());

    if (bytes_read == 0) {
      if (running_.load(std::memory_order_relaxed)) {
        spdlog::warn("Hyprland IPC: Socket closed by peer, reconnecting");
      }
      return;
    }

    if (bytes_read < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (running_.load(std::memory_order_relaxed)) {
        spdlog::error("Hyprland IPC: read failed: {}", std::strerror(errno));
      }
      return;
    }

    pending.append(buffer.data(), static_cast<std::size_t>(bytes_read));
    for (auto newline_pos = pending.find('\n'); newline_pos != std::string::npos;
         newline_pos = pending.find('\n')) {
      std::string messageReceived = pending.substr(0, newline_pos);
      pending.erase(0, newline_pos + 1);
      if (messageReceived.empty()) {
        continue;
      }
      spdlog::debug("hyprland IPC received {}", messageReceived);
      dispatchEvent(messageReceived);
    }
  }
}

void IPC::closeEventSocket() {
  std::lock_guard<std::mutex> lock(socketMutex_);
  if (socketfd_ != -1) {
    if (close(socketfd_) == -1) {
      spdlog::error("Hyprland IPC: Couldn't close socket");
    }
    socketfd_ = -1;
  }
}

// Waits unless the IPC is being destroyed; returns whether to retry.
bool IPC::waitBeforeRetry(std::chrono::milliseconds delay) {
  std::unique_lock<std::mutex> lock(socketMutex_);
  return !stopRequested_.wait_for(lock, delay,
                                  [this] { return !running_.load(std::memory_order_relaxed); });
}

// Events sent while disconnected are lost. Modules that rebuild on Hyprland's own
// "configreloaded" (hyprland/workspaces) re-initialize as on a config reload; modules that only
// follow focus get the current workspace and window as the events Hyprland would have sent.
// Stops between steps once the IPC is being destroyed.
void IPC::refreshAfterReconnect() {
  const auto stopping = [this] { return !running_.load(std::memory_order_relaxed); };
  try {
    if (stopping()) {
      return;
    }
    dispatchEvent("configreloaded>>");

    if (stopping()) {
      return;
    }
    // Hyprland builds whose workspace JSON has an "address" name workspaces by it in events;
    // earlier ones send the name, and the id in workspacev2.
    const auto workspace = getSocket1JsonReply("activeworkspace");
    if (workspace.isObject() && workspace.isMember("name")) {
      const auto name = workspace["name"].asString();
      if (workspace.isMember("address")) {
        const auto address = workspace["address"].asString();
        dispatchEvent("workspace>>" + address);
        dispatchEvent("workspacev2>>" + address + "," + name);
      } else {
        dispatchEvent("workspace>>" + name);
        dispatchEvent("workspacev2>>" + std::to_string(workspace["id"].asInt()) + "," + name);
      }
    }

    if (stopping()) {
      return;
    }
    // {} while no window is focused, which Hyprland announces as "activewindow>>,".
    const auto window = getSocket1JsonReply("activewindow");
    if (window.isObject()) {
      auto address = window["address"].asString();
      if (address.starts_with("0x")) {
        address.erase(0, 2);
      }
      dispatchEvent("activewindow>>" + window["class"].asString() + "," +
                    window["title"].asString());
      dispatchEvent("activewindowv2>>" + address);
    }
  } catch (const std::exception& e) {
    spdlog::warn("Hyprland IPC: couldn't refresh modules after reconnecting: {}", e.what());
  }
}

void IPC::dispatchEvent(const std::string& ev) {
  try {
    parseIPC(ev);
  } catch (std::exception& e) {
    spdlog::warn("Failed to parse IPC message: {}, reason: {}", ev, e.what());
  }
}

void IPC::parseIPC(const std::string& ev) {
  std::string request = ev.substr(0, ev.find_first_of('>'));
  std::unique_lock lock(callbackMutex_);

  for (auto& [eventname, handler] : callbacks_) {
    if (eventname == request) {
      handler->onEvent(ev);
    }
  }
}

void IPC::registerForIPC(const std::string& ev, EventHandler* ev_handler) {
  if (ev_handler == nullptr) {
    return;
  }

  std::unique_lock lock(callbackMutex_);
  callbacks_.emplace_back(ev, ev_handler);
}

void IPC::unregisterForIPC(EventHandler* ev_handler) {
  if (ev_handler == nullptr) {
    return;
  }

  std::unique_lock lock(callbackMutex_);

  for (auto it = callbacks_.begin(); it != callbacks_.end();) {
    auto& [eventname, handler] = *it;
    if (handler == ev_handler) {
      callbacks_.erase(it++);
    } else {
      ++it;
    }
  }
}

std::string IPC::getSocket1Reply(const std::string& rq) {
  // basically hyprctl

  util::ScopedFd serverSocket(socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0));

  if (serverSocket < 0) {
    throw std::runtime_error("Hyprland IPC: Couldn't open a socket (1)");
  }

  // SO_SNDTIMEO also bounds connect() while Hyprland's accept backlog is full.
  if (setsockopt(serverSocket.get(), SOL_SOCKET, SO_SNDTIMEO, &kRequestTimeout,
                 sizeof(kRequestTimeout)) == -1 ||
      setsockopt(serverSocket.get(), SOL_SOCKET, SO_RCVTIMEO, &kRequestTimeout,
                 sizeof(kRequestTimeout)) == -1) {
    throw std::runtime_error("Hyprland IPC: Couldn't set the socket timeouts (7)");
  }

  // get the instance signature
  auto* instanceSig = getenv("HYPRLAND_INSTANCE_SIGNATURE");

  if (instanceSig == nullptr) {
    throw std::runtime_error(
        "Hyprland IPC: HYPRLAND_INSTANCE_SIGNATURE was not set! (Is Hyprland running?)");
  }

  sockaddr_un serverAddress = {0};
  serverAddress.sun_family = AF_UNIX;

  std::string socketPath = IPC::getSocketFolder(instanceSig) / ".socket.sock";

  // Use snprintf to copy the socketPath string into serverAddress.sun_path
  const auto socketPathLength =
      snprintf(serverAddress.sun_path, sizeof(serverAddress.sun_path), "%s", socketPath.c_str());
  if (socketPathLength < 0 ||
      socketPathLength >= static_cast<int>(sizeof(serverAddress.sun_path))) {
    throw std::runtime_error("Hyprland IPC: Couldn't copy socket path (6)");
  }

  if (connect(serverSocket, reinterpret_cast<sockaddr*>(&serverAddress), sizeof(serverAddress)) <
      0) {
    throw std::runtime_error("Hyprland IPC: Couldn't connect to " + socketPath + ". (3)");
  }

  std::size_t totalWritten = 0;
  while (totalWritten < rq.length()) {
    const auto sizeWritten =
        write(serverSocket, rq.c_str() + totalWritten, rq.length() - totalWritten);

    if (sizeWritten < 0) {
      if (errno == EINTR) {
        continue;
      }
      spdlog::error("Hyprland IPC: Couldn't write (4)");
      return "";
    }
    if (sizeWritten == 0) {
      spdlog::error("Hyprland IPC: Socket write made no progress");
      return "";
    }
    totalWritten += static_cast<std::size_t>(sizeWritten);
  }

  std::array<char, 8192> buffer = {0};
  std::string response;
  ssize_t sizeWritten = 0;

  do {
    sizeWritten = read(serverSocket, buffer.data(), 8192);

    if (sizeWritten < 0) {
      spdlog::error("Hyprland IPC: Couldn't read (5)");
      return "";
    }
    response.append(buffer.data(), sizeWritten);
  } while (sizeWritten > 0);

  return response;
}

Json::Value IPC::getSocket1JsonReply(const std::string& rq) {
  std::string reply = getSocket1Reply("j/" + rq);

  if (reply.empty()) {
    return {};
  }

  return parser_.parse(reply);
}

bool IPC::isLuaConfigProvider(const std::string& systemInfo) {
  // Hyprland reports which config manager it actually loaded in "systeminfo",
  // as a "configProvider: lua" / "configProvider: hyprlang" line. That is the
  // authoritative signal: the version alone is not enough, because Hyprland
  // only uses the Lua manager when the config file name ends in ".lua", so a
  // >= 0.54 instance started with a traditional hyprland.conf still speaks the
  // legacy dispatch protocol. The field landed together with the Lua config
  // manager in 0.55, so its absence means the instance predates Lua support
  // entirely and necessarily speaks the legacy protocol.
  static constexpr std::string_view key = "configProvider:";

  const size_t keyPos = systemInfo.find(key);
  if (keyPos == std::string::npos) {
    return false;
  }

  const size_t valuePos = keyPos + key.size();
  const size_t lineEnd = systemInfo.find('\n', valuePos);
  const std::string value = lineEnd == std::string::npos
                                ? systemInfo.substr(valuePos)
                                : systemInfo.substr(valuePos, lineEnd - valuePos);

  return trim(value) == "lua";
}

bool IPC::isLuaProtocol() {
  if (s_luaProtocolDetected_.has_value()) {
    return *s_luaProtocolDetected_;
  }

  // The query is read-only, so detection has none of the side effects of an
  // actual dispatch probe. Only a reply decides the protocol: after a failed
  // request (unreachable, or timed out) assume legacy for this dispatch and
  // detect again on the next.
  std::string systemInfo;
  try {
    systemInfo = getSocket1Reply("systeminfo");
  } catch (const std::exception& e) {
    spdlog::warn("Hyprland IPC: could not read systeminfo ({}), assuming legacy protocol",
                 e.what());
    return false;
  }
  if (systemInfo.empty()) {
    spdlog::warn("Hyprland IPC: no systeminfo reply, assuming legacy protocol");
    return false;
  }

  const bool luaProto = isLuaConfigProvider(systemInfo);
  if (luaProto) {
    spdlog::info("Hyprland IPC: detected Lua-based dispatch protocol");
  } else {
    spdlog::info("Hyprland IPC: detected legacy dispatch protocol");
  }

  s_luaProtocolDetected_ = luaProto;
  return luaProto;
}

std::string IPC::buildLuaDispatch(const std::string& dispatcher, const std::string& arg) {
  // Map old-style dispatchers to the new Lua hl.dsp API.
  //
  // Old format:  dispatch workspace 1
  // New format:  /dispatch hl.dsp.focus({ workspace = "1" })
  //
  // Old format:  dispatch focusworkspaceoncurrentmonitor 2
  // New format:  /dispatch hl.dsp.focus({ workspace = "2", on_current_monitor = true })
  //
  // Old format:  dispatch togglespecialworkspace name
  // New format:  /dispatch hl.dsp.workspace.toggle_special("name")

  if (dispatcher == "workspace") {
    return "/dispatch hl.dsp.focus({ workspace = \"" + arg + "\" })";
  }
  if (dispatcher == "focusworkspaceoncurrentmonitor") {
    return "/dispatch hl.dsp.focus({ workspace = \"" + arg + "\", on_current_monitor = true })";
  }
  if (dispatcher == "togglespecialworkspace") {
    if (arg.empty()) {
      return "/dispatch hl.dsp.workspace.toggle_special()";
    }
    return "/dispatch hl.dsp.workspace.toggle_special(\"" + arg + "\")";
  }

  // Fallback for any other dispatcher: try the old format wrapped in dispatch().
  // This may not work for all dispatchers, but it's a reasonable default.
  spdlog::warn("Hyprland IPC: unknown dispatcher '{}' in Lua mode, attempting generic format",
               dispatcher);
  return "/dispatch hl.dsp." + dispatcher + "(\"" + arg + "\")";
}

std::string IPC::dispatch(const std::string& dispatcher, const std::string& arg) {
  if (isLuaProtocol()) {
    return getSocket1Reply(buildLuaDispatch(dispatcher, arg));
  }
  // Legacy format: "dispatch <dispatcher> <arg>"
  std::string cmd = "dispatch " + dispatcher;
  if (!arg.empty()) {
    cmd += " " + arg;
  }
  return getSocket1Reply(cmd);
}

}  // namespace waybar::modules::hyprland
