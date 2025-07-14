package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.common.util.UserAgentManager.determineApplicationName;
import static com.databricks.jdbc.common.util.UserAgentManager.getUserAgentString;
import static com.databricks.jdbc.common.util.UserAgentManager.updateUserAgentAndTelemetry;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
  }

  @AfterEach
  public void tearDown() {
    telemetryHelperMock.close();
  }

  private static Stream<Arguments> provideApplicationNameTestCases() {
    return Stream.of(
        // Test case 1: UserAgentEntry takes precedence
        Arguments.of(
            "MyUserAgent",
            "AppNameValue",
            "ClientInfoApp",
            "MyUserAgent",
            "When useragententry is set"),
        // Test case 2: ApplicationName is used when UserAgentEntry is null
        Arguments.of(
            null,
            "AppNameValue",
            "ClientInfoApp",
            "AppNameValue",
            "When useragententry is not set but applicationname is"),
        // Test case 3: ClientInfo is used when both UserAgentEntry and ApplicationName are null
        Arguments.of(
            null,
            null, // applicationName
            "ClientInfoApp",
            "ClientInfoApp",
            "When URL params are not set but client info is provided"));
  }

  @ParameterizedTest
  @MethodSource("provideApplicationNameTestCases")
  public void testDetermineApplicationName(
      String customerUserAgent,
      String applicationName,
      String clientInfoApp,
      String expectedResult) {
    // Setup only necessary stubs
    Mockito.lenient().when(connectionContext.getCustomerUserAgent()).thenReturn(customerUserAgent);
    Mockito.lenient().when(connectionContext.getApplicationName()).thenReturn(applicationName);

    // Execute
    String result = determineApplicationName(connectionContext, clientInfoApp);

    // Verify
    assertEquals(expectedResult, result);
  }

  @Test
  public void testDetermineApplicationName_WithSystemProperty() {
    // When falling back to system property
    when(connectionContext.getCustomerUserAgent()).thenReturn(null);
    when(connectionContext.getApplicationName()).thenReturn(null);

    System.setProperty("app.name", "SystemPropApp");
    try {
      String result = determineApplicationName(connectionContext, null);
      assertEquals("SystemPropApp", result);
    } finally {
      System.clearProperty("app.name");
    }
  }

  @Test
  public void testUpdateUserAgentAndTelemetry() {
    // Test that both telemetry and user agent are updated
    when(connectionContext.getCustomerUserAgent()).thenReturn("TestApp");
    //    when(UserAgent.sanitize("version")).thenReturn("version");

    updateUserAgentAndTelemetry(connectionContext, null);

    telemetryHelperMock.verify(() -> TelemetryHelper.updateClientAppName("TestApp"));
    String userAgent = getUserAgentString();
    assertTrue(userAgent.contains("TestApp/version"));
  }

  @Test
  public void testUpdateUserAgentAndTelemetry_WithVersion() {
    // Test with app name containing version
    when(connectionContext.getCustomerUserAgent()).thenReturn("MyApp/1.2.3");
    //    when(UserAgent.sanitize("1.2.3")).thenReturn("1.2.3");

    updateUserAgentAndTelemetry(connectionContext, null);

    telemetryHelperMock.verify(() -> TelemetryHelper.updateClientAppName("MyApp/1.2.3"));
    String userAgent = getUserAgentString();
    assertTrue(userAgent.contains("MyApp/1.2.3"));
  }

  @Test
  void testUserAgentSetsClientCorrectly() throws DatabricksSQLException {
    // Thrift with all-purpose cluster
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(CLUSTER_JDBC_URL, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    String userAgent = getUserAgentString();
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/1.0.7-oss"));
    assertTrue(userAgent.contains(" Java/THttpClient"));
    assertTrue(userAgent.contains(" MyApp/version"));
    assertTrue(userAgent.contains(" databricks-jdbc-http "));
    assertFalse(userAgent.contains("databricks-sdk-java"));

    // Thrift with warehouse
    connectionContext =
        DatabricksConnectionContextFactory.create(WAREHOUSE_JDBC_URL, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    userAgent = getUserAgentString();
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/1.0.7-oss"));
    assertTrue(userAgent.contains(" Java/THttpClient"));
    assertTrue(userAgent.contains(" MyApp/version"));
    assertTrue(userAgent.contains(" databricks-jdbc-http "));
    assertFalse(userAgent.contains("databricks-sdk-java"));

    // SEA
    connectionContext =
        DatabricksConnectionContextFactory.create(WAREHOUSE_JDBC_URL_WITH_SEA, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    userAgent = getUserAgentString();
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/1.0.7-oss"));
    assertTrue(userAgent.contains(" Java/SQLExecHttpClient"));
    assertTrue(userAgent.contains(" databricks-jdbc-http "));
    assertFalse(userAgent.contains("databricks-sdk-java"));
  }

  @Test
  void testUserAgentSetsCustomerInput() throws DatabricksSQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(USER_AGENT_URL, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    String userAgent = getUserAgentString();
    assertTrue(userAgent.contains("TEST/24.2.0.2712019"));
  }
}
