package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.common.util.UserAgentManager.USER_AGENT_SEA_CLIENT;
import static com.databricks.jdbc.common.util.UserAgentManager.getUserAgentString;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.telemetry.TelemetryHelper;
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
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/3.0.5"));
    assertTrue(userAgent.contains(" Java/THttpClient"));
    assertTrue(userAgent.contains(" MyApp/version"));
    assertTrue(userAgent.contains(" databricks-jdbc-http "));
    assertFalse(userAgent.contains("databricks-sdk-java"));

    // Thrift with warehouse
    connectionContext =
        DatabricksConnectionContextFactory.create(WAREHOUSE_JDBC_URL, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    userAgent = getUserAgentString();
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/3.0.5"));
    assertTrue(userAgent.contains(" Java/THttpClient"));
    assertTrue(userAgent.contains(" MyApp/version"));
    assertTrue(userAgent.contains(" databricks-jdbc-http "));
    assertFalse(userAgent.contains("databricks-sdk-java"));

    // SEA
    connectionContext =
        DatabricksConnectionContextFactory.create(WAREHOUSE_JDBC_URL_WITH_SEA, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    userAgent = getUserAgentString();
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/3.0.5"));
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
