package(
    default_visibility = ["//src:__subpackages__"],
)

java_library(
    name = "remote",
    srcs = glob(["*.java"], ["CacheNotFoundException.java", "RemoteActionCache.java", "HazelcastCacheFactory.java", "RemoteOptions.java", "MemcacheActionCache.java"]),
    deps = [
        "//src/main/java/com/google/devtools/build/lib:build-base",
        "//src/main/java/com/google/devtools/build/lib:concurrent",
        "//src/main/java/com/google/devtools/build/lib:events",
        "//src/main/java/com/google/devtools/build/lib:io",
        "//src/main/java/com/google/devtools/build/lib:packages-internal",
        "//src/main/java/com/google/devtools/build/lib:runtime",
        "//src/main/java/com/google/devtools/build/lib:util",
        "//src/main/java/com/google/devtools/build/lib:vfs",
        "//src/main/java/com/google/devtools/build/lib/actions",
        "//src/main/java/com/google/devtools/build/lib/standalone",
        "//src/main/java/com/google/devtools/common/options",
        "//src/main/protobuf:remote_protocol_java_proto",
        "//third_party:apache_httpclient",
        "//third_party:apache_httpcore",
        "//third_party:gson",
        "//third_party:guava",
        "//third_party:hazelcast",
        "//third_party/protobuf",
        ":remote-cache"
    ],
)

java_library(
    name = "remote-cache",
    srcs = ["CacheNotFoundException.java", "RemoteActionCache.java", "HazelcastCacheFactory.java", "RemoteOptions.java", "MemcacheActionCache.java"],
    deps = [
        "//src/main/java/com/google/devtools/build/lib:build-base",
        "//src/main/java/com/google/devtools/build/lib:concurrent",
        "//src/main/java/com/google/devtools/build/lib:events",
        "//src/main/java/com/google/devtools/build/lib:io",
        "//src/main/java/com/google/devtools/build/lib:packages-internal",
        "//src/main/java/com/google/devtools/build/lib:runtime",
        "//src/main/java/com/google/devtools/build/lib:util",
        "//src/main/java/com/google/devtools/build/lib:vfs",
        "//src/main/java/com/google/devtools/build/lib/actions",
        "//src/main/java/com/google/devtools/common/options",
        "//src/main/protobuf:remote_protocol_java_proto",
        "//third_party:apache_httpclient",
        "//third_party:apache_httpcore",
        "//third_party:gson",
        "//third_party:guava",
        "//third_party:hazelcast",
        "//third_party/protobuf",
    ],
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)
