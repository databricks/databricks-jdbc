package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FrontendLogContext {
  @JsonProperty("client_context")
  TelemetryClientContext clientContext;

  public FrontendLogContext() {}

  public TelemetryClientContext getClientContext() {
    return clientContext;
  }

  public FrontendLogContext setClientContext(TelemetryClientContext clientContext) {
    this.clientContext = clientContext;
    return this;
  }
}
