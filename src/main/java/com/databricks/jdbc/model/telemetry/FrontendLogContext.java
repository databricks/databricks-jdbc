package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FrontendLogContext {
    @JsonProperty("client_context")
    ClientContext clientContext;

    public FrontendLogContext() {}

    public ClientContext getClientContext() {
        return clientContext;
    }

    public FrontendLogContext setClientContext(ClientContext clientContext) {
        this.clientContext = clientContext;
        return this;
    }
}
