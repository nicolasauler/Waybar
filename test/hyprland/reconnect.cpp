#if __has_include(<catch2/catch_test_macros.hpp>)
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#else
#include <catch2/catch.hpp>
#endif

#include <poll.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "modules/hyprland/backend.hpp"
#include "util/scoped_fd.hpp"

namespace fs = std::filesystem;
namespace hyprland = waybar::modules::hyprland;
using namespace std::chrono_literals;

namespace {

constexpr const char* kSignature = "reconnect_test";

using Fd = waybar::util::ScopedFd;

// Sets an environment variable and restores the previous value.
class ScopedEnv {
 public:
  ScopedEnv(const char* name, const std::string& value) : name_(name) {
    if (const char* old = std::getenv(name)) {
      old_ = old;
    }
    setenv(name, value.c_str(), 1);
  }
  ScopedEnv(const ScopedEnv&) = delete;
  ScopedEnv& operator=(const ScopedEnv&) = delete;
  ~ScopedEnv() {
    if (old_) {
      setenv(name_, old_->c_str(), 1);
    } else {
      unsetenv(name_);
    }
  }

 private:
  const char* name_;
  std::optional<std::string> old_;
};

class TempDir {
 public:
  explicit TempDir(fs::path path) : path_(std::move(path)) {
    fs::remove_all(path_);
    fs::create_directories(path_);
  }
  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;
  ~TempDir() {
    std::error_code ignored;
    fs::remove_all(path_, ignored);
  }

  const fs::path& path() const { return path_; }

 private:
  fs::path path_;
};

Fd listenOn(const fs::path& path, int backlog = 8) {
  Fd fd(socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0));
  REQUIRE(fd.get() != -1);
  sockaddr_un addr = {};
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
  REQUIRE(bind(fd.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
  REQUIRE(listen(fd.get(), backlog) == 0);
  return fd;
}

// The next connection within the timeout, or an invalid Fd.
Fd acceptWithin(int listener, std::chrono::milliseconds timeout) {
  pollfd pfd{.fd = listener, .events = POLLIN, .revents = 0};
  if (poll(&pfd, 1, static_cast<int>(timeout.count())) != 1) {
    return Fd();
  }
  return Fd(accept4(listener, nullptr, nullptr, SOCK_CLOEXEC));
}

void send(const Fd& fd, const std::string& event) {
  const std::string line = event + "\n";
  REQUIRE(write(fd.get(), line.data(), line.size()) == static_cast<ssize_t>(line.size()));
}

// Stands in for Hyprland: the test drives socket2, and socket1 answers the queries a listener
// makes after reconnecting (or accepts them and never answers).
class FakeHyprland {
 public:
  explicit FakeHyprland(std::string activeWorkspace, bool withEventSocket = true)
      : dir_(fs::temp_directory_path() / ("waybar-hyprland-reconnect-" + std::to_string(getpid()))),
        runtimeDir_("XDG_RUNTIME_DIR", dir_.path()),
        signature_("HYPRLAND_INSTANCE_SIGNATURE", kSignature),
        activeWorkspace_(std::move(activeWorkspace)) {
    fs::create_directories(instance());
    if (withEventSocket) {
      startEvents();
    }
    requests_ = listenOn(instance() / ".socket.sock");
    replies_ = std::thread([this] { answerRequests(); });
  }

  FakeHyprland(const FakeHyprland&) = delete;
  FakeHyprland& operator=(const FakeHyprland&) = delete;

  ~FakeHyprland() {
    stopping_ = true;
    replies_.join();
  }

  void startEvents(int backlog = 8) { events_ = listenOn(instance() / ".socket2.sock", backlog); }

  Fd acceptEvents(std::chrono::milliseconds timeout) const {
    return acceptWithin(events_.get(), timeout);
  }

  // Fill the accept backlog, as a compositor that stopped accepting would.
  void fillEventBacklog() {
    startEvents(0);
    for (int i = 0; i < 64; ++i) {
      Fd client(socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
      sockaddr_un addr = {};
      addr.sun_family = AF_UNIX;
      std::strncpy(addr.sun_path, (instance() / ".socket2.sock").c_str(),
                   sizeof(addr.sun_path) - 1);
      if (connect(client.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == -1) {
        REQUIRE(errno == EAGAIN);
        return;
      }
      backlog_.push_back(std::move(client));
    }
    FAIL("the accept backlog never filled");
  }

  void stopAnswering() { answering_ = false; }
  // Reads each request and closes it without replying, which ends it as a failure or a timeout
  // does: with an empty reply.
  void closeRequests(bool close) { closing_ = close; }
  int requests() const { return requests_count_; }

 private:
  fs::path instance() const { return dir_.path() / "hypr" / kSignature; }

  void answerRequests() {
    while (!stopping_) {
      Fd client = acceptWithin(requests_.get(), 50ms);
      if (client.get() == -1) {
        continue;
      }
      ++requests_count_;
      if (!answering_) {
        std::lock_guard lock(mutex_);
        unanswered_.push_back(std::move(client));
        continue;
      }
      std::array<char, 256> request{};
      const ssize_t size = read(client.get(), request.data(), request.size() - 1);
      if (closing_) {
        continue;  // read, so that the client's write cannot meet a closed socket
      }
      const std::string rq(request.data(), size > 0 ? static_cast<std::size_t>(size) : 0);
      std::string reply = "{}";
      if (rq == "j/activeworkspace") {
        reply = activeWorkspace_;
      } else if (rq == "j/activewindow") {
        reply = R"({"address": "0x55aa", "class": "kitty", "title": "editor"})";
      } else if (rq == "systeminfo") {
        reply = "Hyprland\nconfigProvider: lua\n";
      }
      [[maybe_unused]] const ssize_t written = write(client.get(), reply.data(), reply.size());
    }
  }

  TempDir dir_;
  ScopedEnv runtimeDir_;
  ScopedEnv signature_;
  std::string activeWorkspace_;
  Fd events_;
  Fd requests_;
  std::vector<Fd> backlog_;
  std::mutex mutex_;
  std::vector<Fd> unanswered_;
  std::atomic<bool> answering_ = true;
  std::atomic<bool> closing_ = false;
  std::atomic<int> requests_count_ = 0;
  std::atomic<bool> stopping_ = false;
  std::thread replies_;
};

class Recorder : public hyprland::EventHandler {
 public:
  void onEvent(const std::string& ev) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      events_.push_back(ev);
    }
    received_.notify_all();
  }

  bool waitFor(const std::string& ev, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    return received_.wait_for(lock, timeout,
                              [&] { return std::ranges::find(events_, ev) != events_.end(); });
  }

 private:
  std::mutex mutex_;
  std::condition_variable received_;
  std::vector<std::string> events_;
};

// IPC::inst() is process-wide; each test runs its own listener against FakeHyprland.
class TestIPC : public hyprland::IPC {
 public:
  TestIPC() : IPC() {}
  // getSocketFolder() keeps the folder of its first lookup.
  static void resetSocketFolder() { socketFolder_.clear(); }
  static void resetLuaProtocolDetection() { s_luaProtocolDetected_.reset(); }
  using hyprland::IPC::isLuaProtocol;
};

void registerRefreshEvents(TestIPC& ipc, Recorder& recorder) {
  for (const auto* event :
       {"test", "configreloaded", "workspace", "workspacev2", "activewindow", "activewindowv2"}) {
    ipc.registerForIPC(event, &recorder);
  }
}

template <typename Duration>
auto elapsed(std::chrono::steady_clock::time_point since) {
  return std::chrono::duration_cast<Duration>(std::chrono::steady_clock::now() - since);
}

// Records log messages, to observe what the listener did rather than assume it from elapsed time.
class LogCapture : public spdlog::sinks::base_sink<std::mutex> {
 public:
  // Waits until at least `count` messages contain `text`.
  bool waitFor(std::string_view text, std::chrono::milliseconds timeout, std::size_t count = 1) {
    std::unique_lock<std::mutex> lock(linesMutex_);
    return logged_.wait_for(lock, timeout, [&] {
      return static_cast<std::size_t>(std::ranges::count_if(lines_, [&](auto const& line) {
               return line.find(text) != std::string::npos;
             })) >= count;
    });
  }

 protected:
  void sink_it_(const spdlog::details::log_msg& msg) override {
    {
      std::lock_guard<std::mutex> lock(linesMutex_);
      lines_.emplace_back(msg.payload.data(), msg.payload.size());
    }
    logged_.notify_all();
  }
  void flush_() override {}

 private:
  std::mutex linesMutex_;
  std::condition_variable logged_;
  std::vector<std::string> lines_;
};

// Adds a LogCapture to the default logger while in scope; declare it before the IPC it observes.
class ScopedLogCapture {
 public:
  ScopedLogCapture() : sink_(std::make_shared<LogCapture>()) {
    spdlog::default_logger()->sinks().push_back(sink_);
  }
  ScopedLogCapture(const ScopedLogCapture&) = delete;
  ScopedLogCapture& operator=(const ScopedLogCapture&) = delete;
  ~ScopedLogCapture() { std::erase(spdlog::default_logger()->sinks(), sink_); }

  bool waitFor(std::string_view text, std::chrono::milliseconds timeout, std::size_t count = 1) {
    return sink_->waitFor(text, timeout, count);
  }

 private:
  std::shared_ptr<LogCapture> sink_;
};

}  // namespace

TEST_CASE("Hyprland IPC reconnects after Hyprland drops the event socket", "[socketListener]") {
  // Hyprland names workspaces by their address in events: a renamed numbered workspace keeps its
  // number, and a named workspace has no id.
  auto [activeWorkspace, workspaceEvent, workspacev2Event] =
      GENERATE(table<std::string, std::string, std::string>({
          {R"({"address": "3", "id": 3, "name": "web"})", "workspace>>3", "workspacev2>>3,web"},
          {R"({"address": "dev", "name": "dev"})", "workspace>>dev", "workspacev2>>dev,dev"},
      }));
  FakeHyprland hyprland(activeWorkspace);
  TestIPC::resetSocketFolder();
  Recorder recorder;
  std::optional<TestIPC> ipc(std::in_place);
  registerRefreshEvents(*ipc, recorder);

  Fd first = hyprland.acceptEvents(5s);
  REQUIRE(first.get() != -1);
  send(first, "test>>before");
  REQUIRE(recorder.waitFor("test>>before", 5s));

  // What Hyprland does to a client whose event queue overflows.
  first.reset();

  Fd second = hyprland.acceptEvents(5s);
  REQUIRE(second.get() != -1);
  // Events missed while disconnected are replaced by the current state.
  CHECK(recorder.waitFor("configreloaded>>", 5s));
  CHECK(recorder.waitFor(workspaceEvent, 5s));
  CHECK(recorder.waitFor(workspacev2Event, 5s));
  CHECK(recorder.waitFor("activewindow>>kitty,editor", 5s));
  CHECK(recorder.waitFor("activewindowv2>>55aa", 5s));

  send(second, "test>>after");
  CHECK(recorder.waitFor("test>>after", 5s));
  ipc.reset();
}

TEST_CASE("Hyprland IPC refreshes when its first connection comes late", "[socketListener]") {
  // Workspace JSON without an address: the name goes in "workspace", the id in "workspacev2".
  FakeHyprland hyprland(R"({"id": 3, "name": "3"})", false);
  TestIPC::resetSocketFolder();
  ScopedLogCapture logs;
  Recorder recorder;
  std::optional<TestIPC> ipc(std::in_place);
  registerRefreshEvents(*ipc, recorder);

  // Modules have read Hyprland's state; events they miss until the event socket exists count.
  REQUIRE(logs.waitFor("Unable to connect", 5s));
  hyprland.startEvents();

  Fd events = hyprland.acceptEvents(5s);
  REQUIRE(events.get() != -1);
  CHECK(recorder.waitFor("configreloaded>>", 5s));
  CHECK(recorder.waitFor("workspace>>3", 5s));
  CHECK(recorder.waitFor("workspacev2>>3,3", 5s));
  ipc.reset();
}

TEST_CASE("Destroying the Hyprland IPC interrupts its reconnect delay", "[socketListener]") {
  FakeHyprland hyprland(R"({"address": "1", "id": 1, "name": "1"})");
  TestIPC::resetSocketFolder();
  ScopedLogCapture logs;
  std::optional<TestIPC> ipc(std::in_place);

  // Connections that close at once back off: 100, 200, 400 and 800 ms, then 1.6 s.
  auto last = std::chrono::steady_clock::now();
  std::chrono::milliseconds lastGap{};
  for (int accepted = 0; accepted < 5; ++accepted) {
    Fd connection = hyprland.acceptEvents(5s);
    REQUIRE(connection.get() != -1);
    lastGap = elapsed<std::chrono::milliseconds>(last);
    last = std::chrono::steady_clock::now();
  }
  CHECK(lastGap >= 700ms);
  // The listener saw the fifth close; let it enter the 1.6 s wait that follows.
  REQUIRE(logs.waitFor("Socket closed by peer", 5s, 5));
  std::this_thread::sleep_for(100ms);

  const auto start = std::chrono::steady_clock::now();
  ipc.reset();
  CHECK(elapsed<std::chrono::milliseconds>(start) < 500ms);
}

TEST_CASE("Hyprland IPC does not block on a full accept backlog", "[socketListener]") {
  FakeHyprland hyprland(R"({"address": "1", "id": 1, "name": "1"})", false);
  hyprland.fillEventBacklog();
  TestIPC::resetSocketFolder();
  ScopedLogCapture logs;
  std::optional<TestIPC> ipc(std::in_place);

  // The connect attempt failed (EAGAIN) instead of blocking; the listener backs off.
  REQUIRE(logs.waitFor("Unable to connect", 5s));
  const auto start = std::chrono::steady_clock::now();
  ipc.reset();
  CHECK(elapsed<std::chrono::milliseconds>(start) < 500ms);
}

TEST_CASE("An unanswered Hyprland request does not block the IPC forever", "[socketListener]") {
  FakeHyprland hyprland(R"({"address": "1", "id": 1, "name": "1"})");
  hyprland.stopAnswering();
  TestIPC::resetSocketFolder();
  std::optional<TestIPC> ipc(std::in_place);

  Fd first = hyprland.acceptEvents(5s);
  REQUIRE(first.get() != -1);
  first.reset();
  // The refresh after reconnecting asks for the active workspace and gets no answer.
  const auto asked = std::chrono::steady_clock::now();
  while (hyprland.requests() == 0) {
    REQUIRE(elapsed<std::chrono::milliseconds>(asked) < 5000ms);
    std::this_thread::sleep_for(10ms);
  }

  ipc.reset();
  // The request times out after 5 s, and the refresh stops instead of asking further.
  CHECK(elapsed<std::chrono::milliseconds>(asked) < 7000ms);
  CHECK(hyprland.requests() == 1);
}

TEST_CASE("A failed protocol detection is retried rather than cached", "[isLuaProtocol]") {
  FakeHyprland hyprland(R"({"address": "1", "id": 1, "name": "1"})", false);
  TestIPC::resetSocketFolder();
  TestIPC::resetLuaProtocolDetection();

  // A request that fails, or times out, has an empty reply: this dispatch falls back to legacy.
  hyprland.closeRequests(true);
  CHECK_FALSE(TestIPC::isLuaProtocol());

  // Once Hyprland answers, the next dispatch detects its Lua config.
  hyprland.closeRequests(false);
  CHECK(TestIPC::isLuaProtocol());
  CHECK(hyprland.requests() == 2);
  TestIPC::resetLuaProtocolDetection();
}
