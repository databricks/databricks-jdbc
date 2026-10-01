package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.TestConstants.WAREHOUSE_JDBC_URL_WITH_SEA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.util.UserAgentManager;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.UserAgent;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
  void seaIgnoresInvalidCustomerEntry() throws Exception {
    String userAgent = sendRequest("Bad~Name/1.0");
    assertSegmentsAfterOs(userAgent, SEA_CLIENT);
    assertFalse(userAgent.contains("Bad~Name/1.0"));
  }

  @Test
  void seaAddsMarkerAfterClientSwitch() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(
            WAREHOUSE_JDBC_URL_WITH_SEA + "UserAgentEntry=ThoughtSpot", new Properties());
    connectionContext.setClientType(DatabricksClientType.THRIFT);
    HttpClient transport = request -> new Response(request, 200, "OK", Collections.emptyMap());
    HttpClient ordered = ClientConfigurator.withSeaUserAgentOrdering(transport, connectionContext);
    String original =
        "DatabricksJDBCDriverOSS/1.0 databricks-sdk-java/0.118.0 "
            + "jvm/17 os/Linux Java/THttpClient ThoughtSpot/version auth/pat";
    Request request =
        new Request(Request.POST, "https://example.com/api/2.0/sql/statements")
            .withHeader("User-Agent", original);

    ordered.execute(request);
    assertEquals(original, request.getHeaders().get("User-Agent"));

    connectionContext.setClientType(DatabricksClientType.SEA);
    ordered.execute(request);
    assertEquals(
        "DatabricksJDBCDriverOSS/1.0 databricks-sdk-java/0.118.0 "
            + "jvm/17 os/Linux ThoughtSpot/version Java/SQLExecHttpClient "
            + "Java/THttpClient auth/pat",
        request.getHeaders().get("User-Agent"));
  }

  @Test
  void oauthDiscoveryDoesNotResolveClientType() throws Exception {
    HttpClient transport = request -> new Response(request, 200, "OK", Collections.emptyMap());
    // Client type resolution can itself make this OAuth discovery request.
    HttpClient ordered = ClientConfigurator.withSeaUserAgentOrdering(transport, null);
    String userAgent = "Driver/1 sdk/1 jvm/17 os/linux Java/SQLExecHttpClient ThoughtSpot/version";
    Request request =
        new Request(Request.GET, "https://example.com/oidc/.well-known/oauth-authorization-server")
            .withHeader("User-Agent", userAgent);

    ordered.execute(request);

    assertEquals(userAgent, request.getHeaders().get("User-Agent"));
  }

  @Test
  void configuratorInstallsSeaOrdering() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    AtomicReference<String> receivedUserAgent = new AtomicReference<>();
    server.createContext(
        "/",
        exchange -> {
          boolean isTestRequest =
              "/api/2.0/sql/statements/".equals(exchange.getRequestURI().getPath());
          if (isTestRequest) {
            receivedUserAgent.set(exchange.getRequestHeaders().getFirst("User-Agent"));
          }
          exchange.getResponseHeaders().set("Connection", "close");
          exchange.sendResponseHeaders(isTestRequest ? 200 : 404, -1);
          exchange.close();
        });
    server.start();
    try {
      Properties properties = new Properties();
      properties.setProperty("PWD", "test-token");
      String serverUrl = "http://127.0.0.1:" + server.getAddress().getPort();
      IDatabricksConnectionContext connectionContext =
          DatabricksConnectionContextFactory.create(
              "jdbc:databricks://127.0.0.1:"
                  + server.getAddress().getPort()
                  + "/default;transportMode=http;ssl=0;AuthMech=3;"
                  + "httpPath=/sql/1.0/warehouses/warehouse_id;UseThriftClient=0;"
                  + "UserAgentEntry=ThoughtSpot",
              properties);
      try (ClientConfigurator configurator = new ClientConfigurator(connectionContext)) {
        Request request =
            new Request(Request.GET, serverUrl + "/api/2.0/sql/statements/")
                .withHeader(
                    "User-Agent",
                    "Driver/1 sdk/1 jvm/17 os/linux Java/SQLExecHttpClient ThoughtSpot/version");
        Response response = configurator.getDatabricksConfig().getHttpClient().execute(request);
        assertEquals(200, response.getStatusCode());
      }
      assertEquals(
          "Driver/1 sdk/1 jvm/17 os/linux ThoughtSpot/version Java/SQLExecHttpClient",
          receivedUserAgent.get());
    } finally {
      server.stop(0);
    }
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
