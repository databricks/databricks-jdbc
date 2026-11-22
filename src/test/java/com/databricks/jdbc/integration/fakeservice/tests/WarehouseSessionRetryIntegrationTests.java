package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.getFakeServiceHost;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import java.sql.Connection;
import java.sql.DriverManager;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@EnabledIfEnvironmentVariable(named = "FAKE_SERVICE_TYPE", matches = "(?i)SQL_EXEC")
class WarehouseSessionRetryIntegrationTests extends AbstractFakeServiceIntegrationTests {
  private static final String SESSIONS = "/api/2.0/sql/warehouses/retry-warehouse/sessions";

  @ParameterizedTest
  @CsvSource({
    "408, false", "502, false", "503, false", "504, false",
    "408, true", "502, true", "503, true", "504, true"
  })
  void connectionOpensAfterOneHeaderlessCreateSessionFailure(int status, boolean realtime)
      throws Exception {
    FakeServiceExtension service = getDatabricksApiExtension();
    service.stubFor(get(urlPathMatching("/.*\\.well-known/.*")).willReturn(okJson("{}")));
    service.stubFor(
        get(urlPathMatching("/api/2.0/connector-service/feature-flags/.*"))
            .willReturn(okJson("{}")));
    service.stubFor(
        post(urlEqualTo(SESSIONS))
            .inScenario("create-session-retry")
            .whenScenarioStateIs(STARTED)
            .willReturn(aResponse().withStatus(status))
            .willSetStateTo("recovered"));
    service.stubFor(
        post(urlEqualTo(SESSIONS))
            .inScenario("create-session-retry")
            .whenScenarioStateIs("recovered")
            .willReturn(
                okJson("{\"session_id\":\"retry-session\"}")
                    .withHeader("x-databricks-rt-session", Boolean.toString(realtime))));
    String closePath =
        realtime ? SESSIONS + "/retry-session" : "/api/2.0/sql/sessions/retry-session";
    service.stubFor(delete(urlPathEqualTo(closePath)).willReturn(ok()));

    String url =
        "jdbc:databricks://"
            + getFakeServiceHost()
            + "/default;ssl=0;AuthMech=3;UseThriftClient=0;EnableTelemetry=0;"
            + "httpPath=/sql/1.0/warehouses/retry-warehouse";
    try (Connection connection = DriverManager.getConnection(url, "token", "test-token")) {
      assertFalse(connection.isClosed());
      assertEquals(
          realtime,
          ((DatabricksConnection) connection).getSession().getSessionInfo().realtimeSession());
      service.verify(2, postRequestedFor(urlEqualTo(SESSIONS)));
    }
    service.verify(1, deleteRequestedFor(urlPathEqualTo(closePath)));
  }
}
