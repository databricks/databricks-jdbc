package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.TestConstants.WAREHOUSE_JDBC_URL_WITH_SEA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.UserAgentManager;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
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

class DatabricksSdkClientUserAgentTest {
  private static final String SEA_CLIENT = "Java/SQLExecHttpClient";

  @BeforeAll
  static void setUpEarlierEntry() {
    UserAgent.withOtherInfo("EarlierApp", "1.0");
  }

  @Test
  void seaApiClientOrdersCustomerEntry() throws Exception {
    assertSegmentsAfterOs(sendRequest("ThoughtSpot"), "ThoughtSpot/version", SEA_CLIENT);
    assertSegmentsAfterOs(sendRequest("DBeaver%2F25.1"), "DBeaver/25.1", SEA_CLIENT);
  }

  private String sendRequest(String customerUserAgent) throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(
            WAREHOUSE_JDBC_URL_WITH_SEA + "UserAgentEntry=" + customerUserAgent, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    AtomicReference<String> sentUserAgent = new AtomicReference<>();
    HttpClient transport =
        request -> {
          sentUserAgent.set(request.getHeaders().get("User-Agent"));
          return new Response(request, 200, "OK", Collections.emptyMap());
        };
    DatabricksConfig config =
        new DatabricksConfig()
            .setHost("https://example.com")
            .setAuthType("pat")
            .setToken("test-token")
            .setHttpClient(transport);
    ApiClient apiClient =
        DatabricksSdkClient.buildApiClient(
            config,
            UserAgentManager.customerUserAgentSegment(connectionContext.getCustomerUserAgent()),
            true);
    apiClient.execute(new Request(Request.POST, "/api/2.0/sql/statements"), Void.class);
    return sentUserAgent.get();
  }

  @Test
  void seaApiClientRequestsIdentityEncodingWhenCompressionIsDisabled() throws Exception {
    assertEquals("identity", sendRequestAndCaptureEncoding(false));
  }

  @Test
  void seaApiClientLeavesEncodingUnsetWhenCompressionIsEnabled() throws Exception {
    assertNull(sendRequestAndCaptureEncoding(true));
  }

  private String sendRequestAndCaptureEncoding(boolean responseCompressionEnabled)
      throws Exception {
    AtomicReference<String> sentAcceptEncoding = new AtomicReference<>();
    HttpClient transport =
        request -> {
          sentAcceptEncoding.set(request.getHeaders().get("Accept-Encoding"));
          return new Response(request, 200, "OK", Collections.emptyMap());
        };
    DatabricksConfig config =
        new DatabricksConfig()
            .setHost("https://example.com")
            .setAuthType("pat")
            .setToken("test-token")
            .setHttpClient(transport);
    ApiClient apiClient =
        DatabricksSdkClient.buildApiClient(config, null, responseCompressionEnabled);
    apiClient.execute(new Request(Request.POST, "/api/2.0/sql/statements"), Void.class);
    return sentAcceptEncoding.get();
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
}
