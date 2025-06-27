package com.databricks.jdbc.api.impl.volume;

import com.databricks.jdbc.api.impl.VolumeOperationStatus;
import java.util.Objects;

/** Contains the result of a volume put operation. */
public class VolumePutResult {
  private final int statusCode;
  private final VolumeOperationStatus status;
  private final String message;

  /**
   * Constructs a new VolumePutResult.
   *
   * @param statusCode The HTTP status code
   * @param status The operation status
   * @param message Optional error message
   */
  public VolumePutResult(int statusCode, VolumeOperationStatus status, String message) {
    this.statusCode = statusCode;
    this.status = status;
    this.message = message;
  }

  /**
   * Get the HTTP status code.
   *
   * @return the status code
   */
  public int getStatusCode() {
    return statusCode;
  }

  /**
   * Get the operation status.
   *
   * @return the operation status
   */
  public VolumeOperationStatus getStatus() {
    return status;
  }

  /**
   * Get the error message if any.
   *
   * @return the error message or null if successful
   */
  public String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return "VolumePutResult{"
        + "statusCode="
        + statusCode
        + ", status="
        + status
        + (message != null ? ", message='" + message + '\'' : "")
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VolumePutResult that = (VolumePutResult) o;
    return statusCode == that.statusCode
        && status == that.status
        && Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statusCode, status, message);
  }
}
