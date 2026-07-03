package com.databricks.jdbc.comparator.error;

/**
 * Controls whether errors are compared deeply, via the {@code ERROR_COMPARISON_MODE} gate.
 *
 * <p>Modes: {@code off} (legacy class-only check) and {@code shadow} (deep comparison — compare and
 * report). The default is {@code shadow}: deep comparison runs and records DIFF rows in the
 * report/CSV, but a DIFF never fails the run (only a re-thrown Throwable does). Read the active
 * policy via {@link #active()}.
 *
 * <p>Message normalization and tolerance/baseline handling are intentionally omitted — we compare
 * the raw fields (class, SQLState, vendor code, message). (An enforcement mode that fails the run
 * on divergences is out of scope here; it can be added later, from observed shadow-run data.)
 */
public final class ErrorPolicy {

  public enum Mode {
    OFF,
    SHADOW
  }

  private static final String MODE_PROPERTY = "ERROR_COMPARISON_MODE";

  /** Default when the flag is unset, empty, or unrecognized. Shadow reports but never fails. */
  private static final Mode DEFAULT_MODE = Mode.SHADOW;

  private final Mode mode;

  private ErrorPolicy(Mode mode) {
    this.mode = mode;
  }

  /**
   * Resolves the active policy from system properties. Defaults to {@link #DEFAULT_MODE} for null,
   * empty, or unrecognized values (the latter logs a warning) so a misconfigured flag never aborts
   * a comparison run.
   */
  public static ErrorPolicy active() {
    return new ErrorPolicy(parseMode(System.getProperty(MODE_PROPERTY)));
  }

  public static ErrorPolicy of(Mode mode) {
    return new ErrorPolicy(mode);
  }

  private static Mode parseMode(String raw) {
    if (raw == null || raw.isEmpty()) {
      return DEFAULT_MODE;
    }
    switch (raw.trim().toLowerCase()) {
      case "shadow":
        return Mode.SHADOW;
      case "off":
        return Mode.OFF;
      default:
        // Fail safe: a typo in the flag must not abort the comparison run. Fall back to the
        // default mode and warn, rather than throwing from every compare() call.
        System.err.println(
            "[comparator] Unknown ERROR_COMPARISON_MODE '"
                + raw
                + "' (expected off|shadow); defaulting to "
                + DEFAULT_MODE.name().toLowerCase()
                + ".");
        return DEFAULT_MODE;
    }
  }

  public Mode mode() {
    return mode;
  }

  /** True when deep error comparison should run at all (shadow). */
  public boolean isDeepComparisonEnabled() {
    return mode != Mode.OFF;
  }
}
