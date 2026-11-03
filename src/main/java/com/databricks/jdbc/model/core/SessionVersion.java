package com.databricks.jdbc.model.core;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class SessionVersion {
  @JsonProperty("version_id")
  private Long versionId;

  public Long getVersionId() {
    return versionId;
  }

  public SessionVersion setVersionId(Long versionId) {
    this.versionId = versionId;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SessionVersion that = (SessionVersion) o;
    return Objects.equals(versionId, that.versionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(versionId);
  }

  @Override
  public String toString() {
    return new ToStringer(SessionVersion.class).add("versionId", versionId).toString();
  }
}
