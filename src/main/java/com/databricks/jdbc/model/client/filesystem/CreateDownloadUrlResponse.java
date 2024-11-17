package com.databricks.jdbc.model.client.filesystem;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.http.message.BasicHeader;

import java.util.List;

public class CreateDownloadUrlResponse {
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

    @Override
    public String toString() {
        return "CreateDownloadUrlResponse{" +
                "url='" + url + '\'' +
                ", headers=" + headers +
                '}';
    }
}
