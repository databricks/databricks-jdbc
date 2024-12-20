package com.databricks.jdbc.model.client.filesystem;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** CreateDownloadUrlRequest POJO */
public class CreateDownloadUrlRequest {

  @JsonProperty("path")
  private String path;

  public CreateDownloadUrlRequest(String path) {
    this.path = path;
  }

  public String getPath() {
    return this.path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public String toString() {
    return new ToStringer(CreateDownloadUrlRequest.class).add("path", path).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateDownloadUrlRequest that = (CreateDownloadUrlRequest) o;
    return Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(path);
  }
}
