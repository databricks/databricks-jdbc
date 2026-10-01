package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.TestConstants.WAREHOUSE_JDBC_URL_WITH_SEA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.UserAgentManager;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.UserAgent;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class ClientConfiguratorUserAgentTest {
  private static final String SEA_CLIENT = "Java/SQLExecHttpClient";

  @BeforeAll
  static void setUpEarlierEntry() {
    UserAgent.withOtherInfo("EarlierApp", "1.0");
  }

  @Test
  void seaUsesCurrentEntryBeforeClientMarker() throws Exception {
    assertSegmentsAfterOs(sendRequest("ThoughtSpot"), "ThoughtSpot/version", SEA_CLIENT);
    assertSegmentsAfterOs(sendRequest("Omni/1.0"), "Omni/1.0", SEA_CLIENT);
  }

  @Test
  void seaWithoutEntryDoesNotAttributeAnEarlierConnection() throws Exception {
    assertSegmentsAfterOs(sendRequest(null), SEA_CLIENT);
  }

  @Test
  void seaDecodesEncodedEntry() throws Exception {
    assertSegmentsAfterOs(sendRequest("DBeaver%2F25.1"), "DBeaver/25.1", SEA_CLIENT);
  }

  @Test
  void seaRequestSurvivesCustomerEntrySanitizationFailure() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(
            WAREHOUSE_JDBC_URL_WITH_SEA + "UserAgentEntry=Bad~Name/1.0", new Properties());
    HttpClient transport = request -> new Response(request, 200, "OK", Collections.emptyMap());
    HttpClient ordered = ClientConfigurator.withSeaUserAgentOrdering(transport, connectionContext);
    Request request =
        new Request(Request.POST, "https://example.com/api/2.0/sql/statements")
            .withHeader(
                "User-Agent",
                "DatabricksJDBCDriverOSS/1.0 databricks-sdk-java/0.118.0 "
                    + "jvm/17 os/Linux Java/SQLExecHttpClient auth/pat");

    try (MockedStatic<UserAgent> userAgent = mockStatic(UserAgent.class, CALLS_REAL_METHODS)) {
      userAgent
          .when(() -> UserAgent.sanitize("1.0"))
          .thenThrow(new IllegalArgumentException("invalid version"));
      ordered.execute(request);
    }

    assertSegmentsAfterOs(request.getHeaders().get("User-Agent"), SEA_CLIENT);
  }

  private void assertSegmentsAfterOs(String userAgent, String... expected) {
    List<String> segments = Arrays.asList(userAgent.split(" "));
    int osIndex = -1;
    for (int i = 0; i < segments.size(); i++) {
      if (segments.get(i).startsWith("os/")) {
        osIndex = i;
        break;
      }
    }
    assertTrue(osIndex >= 0, userAgent);
    assertEquals(
        Arrays.asList(expected), segments.subList(osIndex + 1, osIndex + 1 + expected.length));
    assertTrue(userAgent.endsWith("auth/pat"), userAgent);
  }

  private String sendRequest(String customerUserAgent) throws Exception {
    String url =
        WAREHOUSE_JDBC_URL_WITH_SEA
            + (customerUserAgent == null ? "" : "UserAgentEntry=" + customerUserAgent);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(url, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    AtomicReference<String> sentUserAgent = new AtomicReference<>();
    HttpClient transport =
        request -> {
          sentUserAgent.set(request.getHeaders().get("User-Agent"));
          return new Response(request, 200, "OK", Collections.emptyMap());
        };
    ApiClient apiClient =
        new ApiClient.Builder()
            .withHttpClient(
                ClientConfigurator.withSeaUserAgentOrdering(transport, connectionContext))
            .withAuthenticateFunc(ignored -> Collections.emptyMap())
            .withGetHostFunc(ignored -> "https://example.com")
            .withGetAuthTypeFunc(ignored -> "pat")
            .build();
    apiClient.execute(
        new Request(Request.POST, "/api/2.0/sql/statements")
            .withHeader("User-Agent", "overwritten by the SDK"),
        Void.class);
    return sentUserAgent.get();
  }
}
