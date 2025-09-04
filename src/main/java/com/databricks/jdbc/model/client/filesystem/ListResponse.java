package com.databricks.jdbc.model.client.filesystem;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class ListResponse {
  @JsonProperty("files")
  List<FileInfo> files;

  public List<FileInfo> getFiles() {
    return files;
  }

  public void setFiles(List<FileInfo> files) {
    this.files = files;
  }

  @Override
  public String toString() {
    return new ToStringer(ListResponse.class).add("files", files).toString();
  }
}
