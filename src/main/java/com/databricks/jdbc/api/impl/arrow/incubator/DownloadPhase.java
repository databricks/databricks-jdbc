package com.databricks.jdbc.api.impl.arrow.incubator;

enum DownloadPhase {
  DATA_DOWNLOAD("data download"),
  DOWNLOAD_SETUP("download setup");

  private final String description;

  DownloadPhase(String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }
}
