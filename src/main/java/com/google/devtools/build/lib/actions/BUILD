load("//tools/build_rules:genproto.bzl", "java_proto_library")

package(
    default_visibility = ["//src:__subpackages__"],
)

# Action graph and cache.
java_library(
    name = "actions",
    srcs = glob([
        "*.java",
        "cache/*.java",
    ]),
    deps = [
        "//src/main/java/com/google/devtools/build/lib:collect",
        "//src/main/java/com/google/devtools/build/lib:common",
        "//src/main/java/com/google/devtools/build/lib:concurrent",
        "//src/main/java/com/google/devtools/build/lib:events",
        "//src/main/java/com/google/devtools/build/lib:io",
        "//src/main/java/com/google/devtools/build/lib:packages-internal",
        "//src/main/java/com/google/devtools/build/lib:shell",
        "//src/main/java/com/google/devtools/build/lib:skylarkinterface",
        "//src/main/java/com/google/devtools/build/lib:unix",
        "//src/main/java/com/google/devtools/build/lib:util",
        "//src/main/java/com/google/devtools/build/lib:vfs",
        "//src/main/java/com/google/devtools/build/skyframe",
        "//src/main/java/com/google/devtools/common/options",
        "//src/main/protobuf:extra_actions_base_java_proto",
        "//third_party:amazon_s3",
        "//third_party:jsr305",
        "//third_party/protobuf",
        ":s3_cache_entry_java_proto",
    ],
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

java_proto_library(
    name = "s3_cache_entry_java_proto",
    src = "cache/S3CacheEntry.proto",
)
