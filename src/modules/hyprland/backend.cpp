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
#include <string>

namespace waybar::modules::hyprland {

namespace {
class ScopedFd {
 public:
  explicit ScopedFd(int fd) : fd_(fd) {}
  ~ScopedFd() {
    if (fd_ != -1) {
      close(fd_);
    }
  }

  // ScopedFd is non-copyable
  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;

  int get() const { return fd_; }

 private:
  int fd_;
};

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

std::filesystem::path IPC::getSocketFolder(const char* instanceSig) {
  static std::mutex folderMutex;
  std::unique_lock lock(folderMutex);

  // socket path, specified by EventManager of Hyprland
  if (!socketFolder_.empty()) {
    return socketFolder_;
  }

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

  socketFolder_ = socketFolder_ / instanceSig;
  return socketFolder_;
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
      dispatch(messageReceived);
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
    dispatch("configreloaded>>");

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
        dispatch("workspace>>" + address);
        dispatch("workspacev2>>" + address + "," + name);
      } else {
        dispatch("workspace>>" + name);
        dispatch("workspacev2>>" + std::to_string(workspace["id"].asInt()) + "," + name);
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
      dispatch("activewindow>>" + window["class"].asString() + "," + window["title"].asString());
      dispatch("activewindowv2>>" + address);
    }
  } catch (const std::exception& e) {
    spdlog::warn("Hyprland IPC: couldn't refresh modules after reconnecting: {}", e.what());
  }
}

void IPC::dispatch(const std::string& ev) {
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

  ScopedFd serverSocket(socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0));

  if (serverSocket.get() < 0) {
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
  if (snprintf(serverAddress.sun_path, sizeof(serverAddress.sun_path), "%s", socketPath.c_str()) <
      0) {
    throw std::runtime_error("Hyprland IPC: Couldn't copy socket path (6)");
  }

  if (connect(serverSocket.get(), reinterpret_cast<sockaddr*>(&serverAddress),
              sizeof(serverAddress)) < 0) {
    throw std::runtime_error("Hyprland IPC: Couldn't connect to " + socketPath + ". (3)");
  }

  auto sizeWritten = write(serverSocket.get(), rq.c_str(), rq.length());

  if (sizeWritten < 0) {
    spdlog::error("Hyprland IPC: Couldn't write (4)");
    return "";
  }

  std::array<char, 8192> buffer = {0};
  std::string response;

  do {
    sizeWritten = read(serverSocket.get(), buffer.data(), 8192);

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

}  // namespace waybar::modules::hyprland
