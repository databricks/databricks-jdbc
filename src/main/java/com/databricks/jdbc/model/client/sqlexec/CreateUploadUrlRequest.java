package com.databricks.jdbc.model.client.sqlexec;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateUploadUrlRequest {
    @JsonProperty("path")
    private String path;

    public CreateUploadUrlRequest(String path) {
      this.path=path;
    }

    public String getPath() {
        return this.path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
