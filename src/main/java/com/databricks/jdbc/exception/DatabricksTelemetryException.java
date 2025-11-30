package com.databricks.jdbc.exception;

/**
 * Exception indicating telemetry export failures. This intentionally does NOT emit telemetry on
 * construction to avoid recursive logging loops.
 */
public class DatabricksTelemetryException extends RuntimeException {
  public DatabricksTelemetryException(String message) {
    super(message);
  }

  public DatabricksTelemetryException(String message, Throwable cause) {
    super(message, cause);
  }
}
