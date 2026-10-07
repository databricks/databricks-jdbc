package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class AgentDetectorTest {

  /** Creates an env lookup function that returns values from the given map. */
  private static java.util.function.Function<String, String> envWith(Map<String, String> env) {
    return env::get;
  }

  @ParameterizedTest
  @MethodSource("singleAgentCases")
  void testDetectsSingleAgent(String envVar, String expectedProduct) {
    Map<String, String> env = new HashMap<>();
    env.put(envVar, "1");
    assertEquals(Optional.of(expectedProduct), AgentDetector.detect(envWith(env)));
  }

  static Stream<Arguments> singleAgentCases() {
    return Stream.of(
        Arguments.of("ANTIGRAVITY_AGENT", AgentDetector.ANTIGRAVITY),
        Arguments.of("CLAUDECODE", AgentDetector.CLAUDE_CODE),
        Arguments.of("CLINE_ACTIVE", AgentDetector.CLINE),
        Arguments.of("CODEX_CI", AgentDetector.CODEX),
        Arguments.of("CURSOR_AGENT", AgentDetector.CURSOR),
        Arguments.of("GEMINI_CLI", AgentDetector.GEMINI_CLI),
        Arguments.of("OPENCODE", AgentDetector.OPEN_CODE));
  }

  @Test
  void testReturnsEmptyWhenNoAgentDetected() {
    Map<String, String> env = new HashMap<>();
    assertFalse(AgentDetector.detect(envWith(env)).isPresent());
  }

  @Test
  void testReturnsEmptyWhenMultipleAgentsDetected() {
    Map<String, String> env = new HashMap<>();
    env.put("CLAUDECODE", "1");
    env.put("CURSOR_AGENT", "1");
    assertFalse(AgentDetector.detect(envWith(env)).isPresent());
  }

  @Test
  void testIgnoresEmptyEnvVarValues() {
    Map<String, String> env = new HashMap<>();
    env.put("CLAUDECODE", "");
    assertFalse(AgentDetector.detect(envWith(env)).isPresent());
  }

  @Test
  void testIgnoresNullEnvVarValues() {
    Map<String, String> env = new HashMap<>();
    env.put("CLAUDECODE", null);
    assertFalse(AgentDetector.detect(envWith(env)).isPresent());
  }

  @Test
  void testAllKnownAgentsAreCovered() {
    // Verify every entry in KNOWN_AGENTS can be detected individually
    for (String[] entry : AgentDetector.KNOWN_AGENTS) {
      Map<String, String> env = new HashMap<>();
      env.put(entry[0], "1");
      assertEquals(
          Optional.of(entry[1]),
          AgentDetector.detect(envWith(env)),
          "Agent with env var " + entry[0] + " should be detected as " + entry[1]);
    }
  }
}
