test_inc = include_directories('../../include')

test_dep = [
    catch2,
    fmt,
    gtkmm,
    jsoncpp,
    spdlog,
]

test_src = files(
    '../main.cpp',
    'backend.cpp',
    'reconnect.cpp',
    '../../src/modules/hyprland/backend.cpp'
)

hyprland_test = executable(
    'hyprland_test',
    test_src,
    dependencies: test_dep,
    include_directories: test_inc,
)

test(
    'hyprland',
    hyprland_test,
    workdir: meson.project_source_root(),
    # A failing refresh waits out several 5 s assertions.
    timeout: 60,
)
