load('@bazel_tools//tools/build_defs/repo:git.bzl', 'git_repository')
git_repository(
    name = "bazel_skylib",
    remote = "https://github.com/bazelbuild/bazel-skylib.git",
    commit = "90ea6feaf33e8ef12fdac9981b2efccc6a25c727",
)
git_repository(
    name = "rules_cc",
    remote = "https://github.com/bazelbuild/rules_cc.git",
    commit = "cb2dfba6746bfa3c3705185981f3109f0ae1b893",
)
git_repository(
    name = "rules_proto",
    remote = "https://github.com/bazelbuild/rules_proto.git",
    commit = "9cd4f8f1ede19d81c6d48910429fe96776e567b1",
)
git_repository(
    name = "com_google_absl",
    remote = "https://github.com/abseil/abseil-cpp.git",
    commit = "325fd7b042ff4ec34f7dd32e602cd81ad0e24b22",
)
git_repository(
    name = "com_github_gflags_gflags",
    remote = "https://github.com/gflags/gflags.git",
    commit = "28f50e0fed19872e0fd50dd23ce2ee8cd759338e",
)
git_repository(
    name = "com_google_glog",
    remote = "https://github.com/google/glog.git",
    commit = "4cc89c9e2b452db579397887c37f302fb28f6ca1",
)
git_repository(
    name = "com_google_protobuf",
    remote = "https://github.com/protocolbuffers/protobuf.git",
    commit = "1363bf9c051af36c555c23d734a204246c215697",
)
load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
protobuf_deps()
git_repository(
    name = "com_google_googletest",
    remote = "https://github.com/google/googletest.git",
    commit = "3f05f651ae3621db58468153e32016bc1397800b",
)

new_local_repository(
    name = "external_libs",
    path = "/usr/include",
    build_file = "external_libs.BUILD.bazel",
)

