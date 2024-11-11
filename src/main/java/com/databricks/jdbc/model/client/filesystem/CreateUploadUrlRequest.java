package com.databricks.jdbc.model.client.filesystem;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class CreateUploadUrlRequest {
  @JsonProperty("path")
  private String path;

  public CreateUploadUrlRequest(String path) {
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
    CreateUploadUrlRequest that = (CreateUploadUrlRequest) o;
    return Objects.equals(path, that.path);
  }

  @Override
  public String toString() {
    return "CreateUploadUrlRequest{" + "path='" + path + '\'' + '}';
  }
}
