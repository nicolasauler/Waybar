#define CATCH_CONFIG_RUNNER
#include <glibmm.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#if __has_include(<catch2/catch_session.hpp>)
#include <catch2/catch_session.hpp>
#include <catch2/catch_version_macros.hpp>
#include <catch2/reporters/catch_reporter_tap.hpp>
#else
#include <catch2/catch.hpp>
#include <catch2/catch_reporter_tap.hpp>
#endif
#include <cstdlib>
#include <memory>

int main(int argc, char* argv[]) {
  // Never reach the Hyprland of the session the tests run in.
  unsetenv("HYPRLAND_INSTANCE_SIGNATURE");
  Catch::Session session;
  Glib::init();

  session.applyCommandLine(argc, argv);
  const auto logger = spdlog::default_logger();
#if CATCH_VERSION_MAJOR >= 3
  for (const auto& spec : session.config().getReporterSpecs()) {
    const auto& reporter_name = spec.name();
#else
  {
    const auto& reporter_name = session.config().getReporterName();
#endif
    if (reporter_name == "tap") {
      spdlog::set_pattern("# [%l] %v");
    } else if (reporter_name == "compact") {
      logger->sinks().clear();
    } else {
      logger->sinks().assign({std::make_shared<spdlog::sinks::stderr_sink_st>()});
    }
  }

  return session.run();
}
