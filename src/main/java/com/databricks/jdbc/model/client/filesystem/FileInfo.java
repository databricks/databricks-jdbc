package com.databricks.jdbc.model.client.filesystem;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FileInfo {
  @JsonProperty("path")
  private String path;

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public String toString() {
    return new ToStringer(FileInfo.class).add("path", path).toString();
  }
}
