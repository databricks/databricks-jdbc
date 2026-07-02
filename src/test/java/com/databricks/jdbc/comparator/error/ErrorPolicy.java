package com.databricks.jdbc.comparator.error;

/**
 * Controls whether errors are compared deeply, via the {@code ERROR_COMPARISON_MODE} gate.
 *
 * <p>Modes advance with the rollout: {@code off} (legacy class-only check) → {@code shadow}
 * (compare and report, never fail CI) → {@code authoritative} (un-baselined error diffs fail the
 * build). Read the active policy via {@link #active()}.
 *
 * <p>Message normalization and tolerance/baseline handling are intentionally omitted for now — we
 * compare the raw fields (class, SQLState, vendor code, message) and will add tolerance later, from
 * observed shadow-run data, only if it proves necessary.
 */
public final class ErrorPolicy {

  public enum Mode {
    OFF,
    SHADOW,
    AUTHORITATIVE
  }

  private static final String MODE_PROPERTY = "ERROR_COMPARISON_MODE";

  private final Mode mode;

  private ErrorPolicy(Mode mode) {
    this.mode = mode;
  }

  /**
   * Resolves the active policy from system properties. Defaults to {@link Mode#OFF} for null,
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
      return Mode.OFF;
    }
    switch (raw.trim().toLowerCase()) {
      case "shadow":
        return Mode.SHADOW;
      case "authoritative":
        return Mode.AUTHORITATIVE;
      case "off":
        return Mode.OFF;
      default:
        // Fail safe: a typo in the flag must not abort the comparison run. Default to OFF
        // (legacy behavior) and warn, rather than throwing from every compare() call.
        System.err.println(
            "[comparator] Unknown ERROR_COMPARISON_MODE '"
                + raw
                + "' (expected off|shadow|authoritative); defaulting to off.");
        return Mode.OFF;
    }
  }

  public Mode mode() {
    return mode;
  }

  /** True when deep error comparison should run at all (shadow or authoritative). */
  public boolean isDeepComparisonEnabled() {
    return mode != Mode.OFF;
  }
}
