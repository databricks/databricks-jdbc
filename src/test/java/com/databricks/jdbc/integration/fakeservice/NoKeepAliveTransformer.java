package com.databricks.jdbc.integration.fakeservice;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformerV2;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import org.apache.http.HttpHeaders;

public class NoKeepAliveTransformer implements ResponseDefinitionTransformerV2 {
  @Override
  public ResponseDefinition transform(ServeEvent serveEvent) {
    return ResponseDefinitionBuilder.like(serveEvent.getResponseDefinition())
        .withHeader(HttpHeaders.CONNECTION, "close")
        .build();
  }

  @Override
  public String getName() {
    return "keep-alive-disabler";
  }
}
