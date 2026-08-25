// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgirpc.launcher;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Golden-vector parity test for {@link LauncherHashing#computeHash}, against the
 * exact {@code (argv, cwd, env) -> hash} pairs the C++ extension's own port checks
 * itself against — {@code vgi/test/cpp/launcher_parity_vectors.hpp}, generated from
 * the Python reference implementation ({@code
 * vgi-rpc-python/scripts/regenerate_launcher_parity_vectors.py}), not hand-written.
 * A hand-written test could pass while silently disagreeing with the actual
 * cross-language contract; these vectors are the contract.
 */
final class LauncherHashingParityTest {

    /** name, argv, cwd, env, expectedHash — transcribed verbatim from launcher_parity_vectors.hpp. */
    static Stream<Arguments> vectors() {
        return Stream.of(
                Arguments.of("empty_argv_empty_env",
                        List.of(), "/tmp", Map.of(), "21499d847854c192"),
                Arguments.of("single_arg",
                        List.of("python"), "/tmp", Map.of(), "13ddf92fa852a381"),
                Arguments.of("many_args",
                        List.of("python", "-m", "foo", "--bar", "baz"), "/tmp", Map.of(),
                        "1d95f2117bce8c2d"),
                Arguments.of("argv_with_spaces",
                        List.of("python", "/path with spaces/foo.py"), "/tmp", Map.of(),
                        "23664770f5414889"),
                Arguments.of("cwd_with_special_chars",
                        List.of("python"), "/tmp/has spaces and \"quotes\"", Map.of(),
                        "e87a8168b8665401"),
                Arguments.of("env_single",
                        List.of("python"), "/tmp", Map.of("VGI_RPC_FOO", "bar"),
                        "70118f0ad5ea8bf3"),
                Arguments.of("env_multiple_sorted_by_python",
                        List.of("python"), "/tmp",
                        Map.of("VGI_RPC_Z", "z", "VGI_RPC_A", "a", "VGI_RPC_M", "m"),
                        "1000503273c593e4"),
                Arguments.of("env_with_quotes_and_backslash",
                        List.of("python"), "/tmp", Map.of("VGI_RPC_FOO", "a\"b\\c"),
                        "f688dc41e1a4416d"),
                Arguments.of("env_value_with_spaces",
                        List.of("python"), "/tmp", Map.of("VGI_RPC_FLAG", "value with spaces"),
                        "48522da323b1a55d"),
                Arguments.of("argv_with_quotes_and_backslash",
                        List.of("echo", "a\"b\\c"), "/tmp", Map.of(),
                        "cfcf140ab2f01b74"),
                Arguments.of("long_path",
                        List.of("/usr/local/bin/very/long/path/to/the/worker/executable"), "/tmp", Map.of(),
                        "b6f2736f279afd0b"),
                Arguments.of("deep_cwd",
                        List.of("python"),
                        "/var/folders/5z/abcdefghijklmnop/T/working/directory/deep/nesting", Map.of(),
                        "a37badbdf41d0559"),
                Arguments.of("many_args_many_env",
                        List.of("java", "-jar", "/opt/foo.jar", "-Dlog.level=INFO"), "/var/folders/work",
                        Map.of("VGI_RPC_TOKEN", "secret", "VGI_RPC_REGION", "us-west-2",
                                "VGI_RPC_BUCKET", "my-bucket"),
                        "8abb635d646af180"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("vectors")
    void matchesThePythonReference(String name, List<String> argv, String cwd,
            Map<String, String> env, String expectedHash) {
        assertEquals(expectedHash, LauncherHashing.computeHash(argv, cwd, env), "vector: " + name);
    }

    @Test
    void nonVgiRpcEnvVarsAreExcludedFromTheHash() {
        // A non-VGI_RPC_ key must not perturb the hash — matches "single_arg"'s
        // vector exactly despite an irrelevant PATH-like variable being present.
        String withNoise = LauncherHashing.computeHash(
                List.of("python"), "/tmp", Map.of("PATH", "/usr/bin", "HOME", "/home/x"));
        assertEquals("13ddf92fa852a381", withNoise);
    }
}
