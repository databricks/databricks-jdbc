package com.databricks.jdbc.common;

/**
 * Telemetry log verbosity for structured driver events; independent of {@link LogLevel} and set via
 * {@code telemetryLogLevel} (see {@link DatabricksJdbcUrlParams#TELEMETRY_LOG_LEVEL}).
 */
public enum TelemetryLogLevel {
  /** Disable telemetry logging. */
  OFF(0),

  /** Unrecoverable conditions that abort processing. */
  FATAL(1),

  /** Errors that fail an operation but do not crash the process. */
  ERROR(2),

  /** Potential issues without interrupting normal flow. */
  WARN(3),

  /** High-level normal activity. */
  INFO(4),

  /** Detailed diagnostics for troubleshooting. */
  DEBUG(5),

  /** Maximum verbosity including internal timings and state. */
  TRACE(6);

  private final int code;

  TelemetryLogLevel(int code) {
    this.code = code;
  }

  /** Returns the integer code for this level. */
  public int toInt() {
    return code;
  }

  /** Parses a telemetry level from its integer code. Defaults to DEBUG for unknown codes. */
  public static TelemetryLogLevel fromInt(int code) {
    switch (code) {
      case 0:
        return OFF;
      case 1:
        return FATAL;
      case 2:
        return ERROR;
      case 3:
        return WARN;
      case 4:
        return INFO;
      case 5:
        return DEBUG;
      case 6:
        return TRACE;
      default:
        return DEBUG;
    }
  }

  /** Parses from string accepting either integer codes or names; falls back to defaultLevel. */
  public static TelemetryLogLevel parse(String value, TelemetryLogLevel defaultLevel) {
    if (value == null) return defaultLevel;
    String v = value.trim();
    if (v.isEmpty()) return defaultLevel;
    try {
      return fromInt(Integer.parseInt(v));
    } catch (NumberFormatException ignored) {
      // not an integer
    }
    try {
      return TelemetryLogLevel.valueOf(v.toUpperCase());
    } catch (IllegalArgumentException ignored) {
      return defaultLevel;
    }
  }
}
