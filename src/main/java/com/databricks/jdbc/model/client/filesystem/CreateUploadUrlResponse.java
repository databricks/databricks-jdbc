package com.databricks.jdbc.model.client.filesystem;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.apache.http.message.BasicHeader;

/** CreateUploadUrlResponse POJO */
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
    return new ToStringer(CreateUploadUrlResponse.class)
        .add("url", url)
        .add("headers", headers)
        .toString();
  }
}
