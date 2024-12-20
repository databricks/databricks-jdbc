package com.databricks.jdbc.model.client.filesystem;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** CreateDeleteUrlRequest POJO */
public class CreateDeleteUrlRequest {

  @JsonProperty("path")
  private String path;

  public CreateDeleteUrlRequest(String path) {
    this.path = path;
  }

  public String getPath() {
    return this.path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateDeleteUrlRequest that = (CreateDeleteUrlRequest) o;
    return Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(path);
  }

  @Override
  public String toString() {
    return new ToStringer(CreateDeleteUrlRequest.class).add("path", path).toString();
  }
}
