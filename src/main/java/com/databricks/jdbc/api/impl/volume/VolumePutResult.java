package com.databricks.jdbc.api.impl.volume;

import com.databricks.jdbc.api.impl.VolumeOperationStatus;
import java.util.Objects;

/** Immutable result of a single PUT upload executed by {@link DBFSVolumeClient#putFiles}. */
public final class VolumePutResult {
  private final String objectPath;
  private final int httpStatus;
  private final VolumeOperationStatus status;
  private final String errorMessage; // null when succeeded

  public VolumePutResult(
      String objectPath, int httpStatus, VolumeOperationStatus status, String errorMessage) {
    this.objectPath = objectPath;
    this.httpStatus = httpStatus;
    this.status = status;
    this.errorMessage = errorMessage;
  }

  public String getObjectPath() {
    return objectPath;
  }

  public int getHttpStatus() {
    return httpStatus;
  }

  public VolumeOperationStatus getStatus() {
    return status;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public boolean isSuccess() {
    return status == VolumeOperationStatus.SUCCEEDED;
  }

  @Override
  public String toString() {
    return "VolumePutResult{"
        + "objectPath='"
        + objectPath
        + '\''
        + ", httpStatus="
        + httpStatus
        + ", status="
        + status
        + (errorMessage != null ? ", errorMessage='" + errorMessage + '\'' : "")
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VolumePutResult that = (VolumePutResult) o;
    return httpStatus == that.httpStatus
        && Objects.equals(objectPath, that.objectPath)
        && status == that.status
        && Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(objectPath, httpStatus, status, errorMessage);
  }
}
