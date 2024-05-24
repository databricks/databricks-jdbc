package com.databricks.jdbc.client.sqlexec;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.Objects;

public class VolumeOperationInfo {

  @JsonProperty("external_links")
  private Collection<ExternalLink> externalLinks;

  @JsonProperty("local_file")
  private String localFile;

  @JsonProperty("volume_operation_type")
  private String volumeOperationType;

  public VolumeOperationInfo setExternalLinks(Collection<ExternalLink> externalLinks) {
    this.externalLinks = externalLinks;
    return this;
  }

  public Collection<ExternalLink> getExternalLinks() {
    return externalLinks;
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
    return Objects.equals(externalLinks, that.externalLinks)
        && Objects.equals(localFile, that.localFile)
        && Objects.equals(volumeOperationType, that.volumeOperationType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(externalLinks, localFile, volumeOperationType);
  }

  @Override
  public String toString() {
    return new ToStringer(VolumeOperationInfo.class)
        .add("externalLinks", externalLinks)
        .add("localFile", localFile)
        .add("volumeOperationType", volumeOperationType)
        .toString();
  }
}
