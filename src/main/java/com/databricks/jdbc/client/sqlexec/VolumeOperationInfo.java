package com.databricks.jdbc.client.sqlexec;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class VolumeOperationInfo {

  @JsonProperty("presigned_url")
  private ExternalLink presignedUrl;

  @JsonProperty("local_file")
  private String localFile;

  @JsonProperty("volume_operation_type")
  private String volumeOperationType;

  public VolumeOperationInfo setPresignedUrl(ExternalLink presignedUrl) {
    this.presignedUrl = presignedUrl;
    return this;
  }

  public ExternalLink getPresignedUrl() {
    return presignedUrl;
  }

  public VolumeOperationInfo setLocalFile(String localFile) {
    this.localFile = localFile;
    return this;
  }

  public String getLocalFile() {
    return localFile;
  }

  public VolumeOperationInfo setVolumeOperationType(String volumeOperationType) {
    this.volumeOperationType = volumeOperationType;
    return this;
  }

  public String getVolumeOperationType() {
    return volumeOperationType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VolumeOperationInfo that = (VolumeOperationInfo) o;
    return Objects.equals(presignedUrl, that.presignedUrl)
        && Objects.equals(localFile, that.localFile)
        && Objects.equals(volumeOperationType, that.volumeOperationType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(presignedUrl, localFile, volumeOperationType);
  }

  @Override
  public String toString() {
    return new ToStringer(VolumeOperationInfo.class)
        .add("presignedUrl", presignedUrl)
        .add("localFile", localFile)
        .add("volumeOperationType", volumeOperationType)
        .toString();
  }
}
