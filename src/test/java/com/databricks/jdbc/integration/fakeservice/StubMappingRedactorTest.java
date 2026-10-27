package com.databricks.jdbc.integration.fakeservice;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StubMappingRedactorTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "/api/2.0/sql/statements/",
        "/api/2.0/sql/warehouses/warehouse/sessions/session/statements",
        "/api/2.0/sql/warehouses/warehouse/sessions/session/statements/statement/result/chunks/1"
      })
  void redactsSignedLinksFromStatementRecordings(String path) {
    StubMapping mapping =
        get(urlEqualTo(path))
            .willReturn(
                okJson(
                        "{\"external_link\":\"https://storage.example.com/result?sig=secret-signature&other=value\"}")
                    .withHeader("Server", "databricks"))
            .build();

    StubMapping redacted = new StubMappingRedactor().transform(mapping, null, Parameters.empty());

    assertEquals(path, redacted.getRequest().getUrl());
    assertFalse(redacted.getResponse().getBody().contains("secret-signature"));
    assertTrue(redacted.getResponse().getBody().contains("[REDACTED]"));
    assertTrue(redacted.getResponse().getBody().contains("other=value"));
  }
}
