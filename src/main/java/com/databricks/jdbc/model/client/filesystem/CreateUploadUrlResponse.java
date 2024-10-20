package com.databricks.jdbc.model.client.filesystem;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.apache.http.message.BasicHeader;

public class CreateUploadUrlResponse {
  @JsonProperty("url")
  private String url;

  @JsonProperty("headers")
  private List<BasicHeader> headers;

  // Getters
  public String getUrl() {
    return this.url;
  }

  public List<BasicHeader> getHeaders() {
    return this.headers;
  }

  // Setters
  public void setUrl(String url) {
    this.url = url;
  }

  public void setHeaders(List<BasicHeader> headers) {
    this.headers = headers;
  }

  // Override toString method
  @Override
  public String toString() {
    return "CreateUploadUrlResponse{" + "url='" + url + '\'' + ", headers=" + headers + '}';
  }
}
