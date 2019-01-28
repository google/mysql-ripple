load('@bazel_tools//tools/build_defs/repo:git.bzl', 'git_repository')
git_repository(
    name = "bazel_skylib",
    remote = "https://github.com/bazelbuild/bazel-skylib.git",
    commit = "e9fc4750d427196754bebb0e2e1e38d68893490a",
)
git_repository(
    name = "com_google_absl",
    remote = "https://github.com/abseil/abseil-cpp.git",
    commit = "e821380d69a549dc64900693942789d21aa4df5e",
)
git_repository(
    name = "com_github_gflags_gflags",
    remote = "https://github.com/gflags/gflags.git",
    commit = "660603a3df1c400437260b51c55490a046a12e8a",
)
git_repository(
    name = "com_google_glog",
    remote = "https://github.com/google/glog.git",
    commit = "8d7a107d68c127f3f494bb7807b796c8c5a97a82",
)
git_repository(
    name = "com_google_protobuf",
    remote = "https://github.com/protocolbuffers/protobuf.git",
    commit = "a6e1cc7e328c45a0cb9856c530c8f6cd23314163",
)
git_repository(
    name = "com_google_googletest",
    remote = "https://github.com/google/googletest.git",
    commit = "9ea01728503a445179353113d2854492f41bee84",
)

new_local_repository(
    name = "external_libs",
    path = "/usr/include",
    build_file = "external_libs.BUILD.bazel",
)

