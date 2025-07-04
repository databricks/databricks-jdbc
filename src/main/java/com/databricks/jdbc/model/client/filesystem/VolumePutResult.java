package com.databricks.jdbc.model.client.filesystem;

import com.databricks.jdbc.api.impl.VolumeOperationStatus;
import java.util.Objects;

/**
 * Contains the result of a volume put operation.
 *
 * <p>This class encapsulates the outcome of uploading files to Databricks volumes, including both
 * successful and failed operations. The HTTP status codes follow standard conventions and indicate
 * the specific type of success or failure.
 *
 * <h3>HTTP Status Codes Used</h3>
 *
 * <ul>
 *   <li><strong>2xx Success Codes:</strong>
 *   <li><strong>4xx Client Error Codes:</strong>
 *       <ul>
 *         <li><code>400 Bad Request</code> - Invalid request parameters (e.g., file not found
 *             locally, mismatched array sizes in batch operations)
 *         <li><code>401 Unauthorized</code> - Authentication failed
 *         <li><code>403 Forbidden</code> - Access denied to the volume or path
 *         <li><code>404 Not Found</code> - Volume or path does not exist
 *         <li><code>429 Too Many Requests</code> - Rate limit exceeded
 *       </ul>
 *   <li><strong>499 Client Closed Request:</strong>
 *       <ul>
 *         <li><code>499</code> - Upload was cancelled by the client before completion
 *       </ul>
 *   <li><strong>5xx Server Error Codes:</strong>
 *       <ul>
 *         <li><code>500 Internal Server Error</code> - Generic server error, network exceptions,
 *             presigned URL generation failures, or upload setup errors
 *         <li><code>502 Bad Gateway</code> - Upstream server error (from storage provider)
 *         <li><code>503 Service Unavailable</code> - Service temporarily unavailable
 *         <li><code>504 Gateway Timeout</code> - Upload timeout
 *       </ul>
 * </ul>
 *
 * <h3>Retry Behavior</h3>
 *
 * <p>Server errors (500, 502, 503, 504) trigger automatic retries with exponential backoff. Client
 * errors (4xx) and cancellations (499) do not trigger retries as they indicate permanent failures
 * or user actions.
 */
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
