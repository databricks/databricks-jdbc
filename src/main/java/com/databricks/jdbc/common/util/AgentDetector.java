package com.databricks.jdbc.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Detects whether the JDBC driver is being invoked by an AI coding agent by checking for well-known
 * environment variables that agents set in their spawned shell processes.
 *
 * <p>Detection only succeeds when exactly one agent environment variable is present, to avoid
 * ambiguous attribution when multiple agent environments overlap.
 *
 * <p>Adding a new agent requires only a new constant and a new entry in {@link #KNOWN_AGENTS}.
 *
 * <p>References for each environment variable:
 *
 * <ul>
 *   <li>ANTIGRAVITY_AGENT: Closed source. Google Antigravity sets this variable.
 *   <li>CLAUDECODE: https://github.com/anthropics/claude-code (sets CLAUDECODE=1)
 *   <li>CLINE_ACTIVE: https://github.com/cline/cline (shipped in v3.24.0)
 *   <li>CODEX_CI: https://github.com/openai/codex (part of UNIFIED_EXEC_ENV array in codex-rs)
 *   <li>CURSOR_AGENT: Closed source. Referenced in a gist by johnlindquist.
 *   <li>GEMINI_CLI: https://google-gemini.github.io/gemini-cli/docs/tools/shell.html (sets
 *       GEMINI_CLI=1)
 *   <li>OPENCODE: https://github.com/opencode-ai/opencode (sets OPENCODE=1)
 * </ul>
 */
public class AgentDetector {

  public static final String ANTIGRAVITY = "antigravity";
  public static final String CLAUDE_CODE = "claude-code";
  public static final String CLINE = "cline";
  public static final String CODEX = "codex";
  public static final String CURSOR = "cursor";
  public static final String GEMINI_CLI = "gemini-cli";
  public static final String OPEN_CODE = "opencode";

  static final String[][] KNOWN_AGENTS = {
    {"ANTIGRAVITY_AGENT", ANTIGRAVITY},
    {"CLAUDECODE", CLAUDE_CODE},
    {"CLINE_ACTIVE", CLINE},
    {"CODEX_CI", CODEX},
    {"CURSOR_AGENT", CURSOR},
    {"GEMINI_CLI", GEMINI_CLI},
    {"OPENCODE", OPEN_CODE},
  };

  /**
   * Detects which AI coding agent (if any) is driving the current process.
   *
   * @return the agent product string if exactly one agent is detected, or empty otherwise
   */
  public static Optional<String> detect() {
    return detect(System::getenv);
  }

  /**
   * Detects which AI coding agent (if any) is present, using the provided function to look up
   * environment variables. This overload exists for testability.
   *
   * @param envLookup function that returns the value of an environment variable, or null if unset
   * @return the agent product string if exactly one agent is detected, or empty otherwise
   */
  static Optional<String> detect(Function<String, String> envLookup) {
    List<String> detected = new ArrayList<>();
    for (String[] entry : KNOWN_AGENTS) {
      String value = envLookup.apply(entry[0]);
      if (value != null && !value.isEmpty()) {
        detected.add(entry[1]);
      }
    }
    if (detected.size() == 1) {
      return Optional.of(detected.get(0));
    }
    return Optional.empty();
  }
}
