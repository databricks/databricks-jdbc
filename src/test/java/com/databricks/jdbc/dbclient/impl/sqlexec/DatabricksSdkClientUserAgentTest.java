package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.TestConstants.WAREHOUSE_JDBC_URL_WITH_SEA;
import static com.databricks.jdbc.common.util.UserAgentManager.MAX_CUSTOMER_USER_AGENT_ENTRIES;
import static com.databricks.jdbc.common.util.UserAgentManager.USER_AGENT_SEA_CLIENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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

  @Test
  void seaApiClientKeepsCustomerEntryPastSharedUserAgentLimit() throws Exception {
    UserAgentManager.resetRegisteredOtherInfo();
    try {
      try (MockedStatic<UserAgent> userAgent =
          Mockito.mockStatic(UserAgent.class, Mockito.CALLS_REAL_METHODS)) {
        // Stubbed so the JVM-wide SDK user agent is not filled with test entries.
        userAgent
            .when(() -> UserAgent.withOtherInfo(anyString(), anyString()))
            .thenAnswer(invocation -> null);
        IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
        when(context.getClientUserAgent()).thenReturn(USER_AGENT_SEA_CLIENT);
        for (int i = 0; i < MAX_CUSTOMER_USER_AGENT_ENTRIES; i++) {
          when(context.getCustomerUserAgent()).thenReturn("LimitApp" + i + "/1.0");
          UserAgentManager.setUserAgent(context);
        }
      }

      String userAgent = sendRequest("OverLimitApp");

      assertFalse(UserAgentManager.getUserAgentString().contains("OverLimitApp"));
      assertSegmentsAfterOs(userAgent, "OverLimitApp/version", SEA_CLIENT);
    } finally {
      UserAgentManager.resetRegisteredOtherInfo();
    }
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
            UserAgentManager.customerUserAgentSegment(connectionContext.getCustomerUserAgent()));
    apiClient.execute(new Request(Request.POST, "/api/2.0/sql/statements"), Void.class);
    return sentUserAgent.get();
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
