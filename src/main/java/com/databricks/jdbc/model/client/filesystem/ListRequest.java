package com.databricks.jdbc.model.client.filesystem;

import com.databricks.sdk.support.QueryParam;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ListRequest {
  @JsonProperty("path")
  @QueryParam("path")
  private String path;

  public ListRequest(String path) {
    this.path = path;
  }

  public String getPath() {
    return path;
  }

  @Override
  public String toString() {
    return new ToStringer(ListRequest.class).add("path", path).toString();
  }
}
