package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.common.util.UserAgentManager.MAX_CUSTOMER_USER_AGENT_ENTRIES;
import static com.databricks.jdbc.common.util.UserAgentManager.USER_AGENT_SEA_CLIENT;
import static com.databricks.jdbc.common.util.UserAgentManager.USER_AGENT_THRIFT_CLIENT;
import static com.databricks.jdbc.common.util.UserAgentManager.getUserAgentString;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.databricks.sdk.core.UserAgent;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class UserAgentManagerTest {

  @Mock private IDatabricksConnectionContext connectionContext;

  private MockedStatic<TelemetryHelper> telemetryHelperMock;

  @BeforeEach
  public void setup() {
    telemetryHelperMock = Mockito.mockStatic(TelemetryHelper.class);
    // Allow keyOf() to call the real method to avoid NPE
    telemetryHelperMock.when(() -> TelemetryHelper.keyOf(Mockito.any())).thenCallRealMethod();
  }

  @AfterEach
  public void tearDown() {
    telemetryHelperMock.close();
  }

  @Test
  public void testUpdateUserAgent() {
    // Test that user agent is updated even without a version
    when(connectionContext.getCustomerUserAgent()).thenReturn("TestApp");
    when(connectionContext.getClientUserAgent()).thenReturn(USER_AGENT_SEA_CLIENT);
    UserAgentManager.setUserAgent(connectionContext);
    String userAgent = getUserAgentString();
    assertTrue(userAgent.contains("TestApp/version"));
  }

  @Test
  public void testEncodedUserAgent() {
    // Test that user agent is updated even if it is encoded
    when(connectionContext.getCustomerUserAgent())
        .thenReturn("DBeaverEncoded%2F25.1.4.202508031529");
    when(connectionContext.getClientUserAgent()).thenReturn(USER_AGENT_SEA_CLIENT);
    UserAgentManager.setUserAgent(connectionContext);
    String userAgent = getUserAgentString();
    assertTrue(userAgent.contains("DBeaverEncoded/25.1.4.202508031529"));
  }

  @Test
  public void testIncorrectUserAgentDoesNotThrowException() {
    when(connectionContext.getCustomerUserAgent()).thenReturn("DBeaverInvalid~25.1.4.202508031529");
    when(connectionContext.getClientUserAgent()).thenReturn(USER_AGENT_SEA_CLIENT);
    UserAgentManager.setUserAgent(connectionContext);
    String userAgent = getUserAgentString();
    assertFalse(userAgent.contains("DBeaverInvalid"));
  }

  @Test
  public void testUserAgentWithSlash() {
    // Test that user agent with added version is updated
    when(connectionContext.getCustomerUserAgent()).thenReturn("MyAppSlash/25.1.4.202508031529");
    when(connectionContext.getClientUserAgent()).thenReturn(USER_AGENT_SEA_CLIENT);
    UserAgentManager.setUserAgent(connectionContext);
    String userAgent = getUserAgentString();
    assertTrue(userAgent.contains("MyAppSlash/25.1.4.202508031529"));
  }

  @Test
  void testUserAgentSetsClientCorrectly() throws DatabricksSQLException {
    // Thrift with all-purpose cluster
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(CLUSTER_JDBC_URL, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    String userAgent = getUserAgentString();
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/"));
    assertTrue(userAgent.contains(" Java/THttpClient"));
    assertTrue(userAgent.contains(" MyApp/version"));
    assertTrue(userAgent.contains(" databricks-jdbc-http "));
    assertFalse(userAgent.contains("databricks-sdk-java"));

    // Thrift with warehouse
    connectionContext =
        DatabricksConnectionContextFactory.create(WAREHOUSE_JDBC_URL, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    userAgent = getUserAgentString();
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/"));
    assertTrue(userAgent.contains(" Java/THttpClient"));
    assertTrue(userAgent.contains(" MyApp/version"));
    assertTrue(userAgent.contains(" databricks-jdbc-http "));
    assertFalse(userAgent.contains("databricks-sdk-java"));

    // SEA
    connectionContext =
        DatabricksConnectionContextFactory.create(WAREHOUSE_JDBC_URL_WITH_SEA, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    userAgent = getUserAgentString();
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/"));
    assertTrue(userAgent.contains(" Java/SQLExecHttpClient"));
    assertTrue(userAgent.contains(" databricks-jdbc-http "));
    assertFalse(userAgent.contains("databricks-sdk-java"));
  }

  @Test
  void testCustomUserAgentIncludedBeforeClientTypeEvaluation() throws DatabricksSQLException {
    // This test verifies that custom user agent is included in connector service requests
    // and maintains proper order: base -> client type -> custom

    // Create connection context with custom user agent entry (use URL without existing UA)
    String jdbcUrlWithCustomUA =
        WAREHOUSE_JDBC_URL_WITH_THRIFT + ";useragententry=CustomTestApp/2.0.0";
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(jdbcUrlWithCustomUA, new Properties());

    // Call setUserAgent
    UserAgentManager.setUserAgent(connectionContext);

    // Get the final user agent string
    String userAgent = getUserAgentString();

    // Verify custom user agent is included
    assertTrue(
        userAgent.contains("CustomTestApp/2.0.0"),
        "Custom user agent should be included: " + userAgent);

    // Verify driver version is present
    assertTrue(
        userAgent.contains("DatabricksJDBCDriverOSS/"),
        "Driver version should be present: " + userAgent);

    // Verify client type is included (proves getClientType was called)
    assertTrue(
        userAgent.contains("Java/THttpClient") || userAgent.contains("Java/SQLExecHttpClient"),
        "Client type should be included: " + userAgent);

    // Verify JDBC HTTP client identifier
    assertTrue(
        userAgent.contains("databricks-jdbc-http"),
        "JDBC HTTP client identifier should be present: " + userAgent);

    // Verify correct order: client type should come BEFORE custom user agent
    int clientTypeIndex =
        userAgent.contains("Java/THttpClient")
            ? userAgent.indexOf("Java/THttpClient")
            : userAgent.indexOf("Java/SQLExecHttpClient");
    int customUAIndex = userAgent.indexOf("CustomTestApp/2.0.0");
    assertTrue(
        clientTypeIndex < customUAIndex,
        "Client type should appear before custom user agent. Order: " + userAgent);
  }

  @Test
  void testUserAgentSetsCustomerInput() throws DatabricksSQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(USER_AGENT_URL, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    String userAgent = getUserAgentString();
    assertTrue(userAgent.contains("TEST/24.2.0.2712019"));
  }

  @Test
  void testRepeatedSetUserAgentDoesNotGrowSdkUserAgent() throws Exception {
    when(connectionContext.getCustomerUserAgent()).thenReturn("RepeatApp/1.0");
    when(connectionContext.getClientUserAgent()).thenReturn(USER_AGENT_THRIFT_CLIENT);
    UserAgentManager.setUserAgent(connectionContext);
    int size = sdkOtherInfo().size();
    String userAgent = getUserAgentString();

    for (int i = 0; i < 1000; i++) {
      UserAgentManager.setUserAgent(connectionContext);
    }

    assertEquals(size, sdkOtherInfo().size());
    assertEquals(userAgent, getUserAgentString());
  }

  @Test
  void testDistinctCustomerUserAgentEntriesAreCapped() throws Exception {
    List<Object> otherInfo = sdkOtherInfo();
    int initialSize = otherInfo.size();
    UserAgentManager.resetRegisteredOtherInfo();
    try {
      when(connectionContext.getClientUserAgent()).thenReturn(USER_AGENT_THRIFT_CLIENT);
      for (int i = 0; i < MAX_CUSTOMER_USER_AGENT_ENTRIES; i++) {
        when(connectionContext.getCustomerUserAgent()).thenReturn("CapApp" + i + "/1.0");
        UserAgentManager.setUserAgent(connectionContext);
      }
      int size = otherInfo.size();

      when(connectionContext.getCustomerUserAgent()).thenReturn("CapAppOverLimit/1.0");
      UserAgentManager.setUserAgent(connectionContext);
      assertEquals(size, otherInfo.size());
      assertFalse(getUserAgentString().contains("CapAppOverLimit"));

      // Client-type entries are not subject to the cap.
      when(connectionContext.getClientUserAgent()).thenReturn(USER_AGENT_SEA_CLIENT);
      UserAgentManager.setUserAgent(connectionContext);
      assertEquals(size + 1, otherInfo.size());
    } finally {
      synchronized (otherInfo) {
        otherInfo.subList(initialSize, otherInfo.size()).clear();
      }
      UserAgentManager.resetRegisteredOtherInfo();
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Object> sdkOtherInfo() throws ReflectiveOperationException {
    Field field = UserAgent.class.getDeclaredField("otherInfo");
    field.setAccessible(true);
    return (List<Object>) field.get(null);
  }
}
