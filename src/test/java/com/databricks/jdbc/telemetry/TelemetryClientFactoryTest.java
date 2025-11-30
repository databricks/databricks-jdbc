package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.common.safe.FeatureFlagTestUtil.enableFeatureFlagForTesting;
import static com.databricks.jdbc.telemetry.TelemetryHelper.TELEMETRY_FEATURE_FLAG_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.TelemetryAuthHelper;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.sdk.core.DatabricksConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TelemetryClientFactoryTest {
  private static final String JDBC_URL_1 =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;UserAgentEntry=MyApp;";
  private static final String JDBC_URL_2 =
      "jdbc:databricks://adb-20.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/ghgjhgj;UserAgentEntry=MyApp;forceEnableTelemetry=1";

  @Mock ClientConfigurator clientConfigurator;
  @Mock DatabricksConfig databricksConfig;

  @BeforeEach
  public void setUp() {
    // Reset the singleton to ensure clean state between tests
    TelemetryClientFactory.getInstance().reset();
  }

  @Test
  public void testGetNoOpTelemetryClient() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL_1, new Properties());
    TelemetryClientFactory.getInstance().closeTelemetryClient(context);
    ITelemetryClient telemetryClient =
        TelemetryClientFactory.getInstance().getTelemetryClient(context);
    assertInstanceOf(NoopTelemetryClient.class, telemetryClient);
    assertEquals(0, TelemetryClientFactory.getInstance().telemetryClientHolders.size());
    assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());
  }

  @Test
  public void testGetAuthenticatedTelemetryClient() throws Exception {
    Properties properties = new Properties();
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL_2, properties);
    when(clientConfigurator.getDatabricksConfig()).thenReturn(databricksConfig);
    setupMocksForTelemetryClient(context);
    ITelemetryClient telemetryClient =
        TelemetryClientFactory.getInstance().getTelemetryClient(context);
    assertInstanceOf(TelemetryClient.class, telemetryClient);
    assertEquals(1, TelemetryClientFactory.getInstance().telemetryClientHolders.size());
    assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());
    TelemetryClientFactory.getInstance().closeTelemetryClient(context);
    assertEquals(0, TelemetryClientFactory.getInstance().telemetryClientHolders.size());
    assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());
    TelemetryClientFactory.getInstance().closeTelemetryClient(context);
  }

  @Test
  public void testGetNoOpTelemetryClientWhenDatabricksConfigIsNull() throws Exception {
    // Create a context with telemetry enabled but no DatabricksConfig
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL_2, new Properties());
    setupMocksForTelemetryClient(context);

    // Mock TelemetryHelper to return null for DatabricksConfig
    try (MockedStatic<TelemetryHelper> mockedStatic = mockStatic(TelemetryHelper.class)) {
      mockedStatic.when(() -> TelemetryHelper.getDatabricksConfigSafely(context)).thenReturn(null);
      mockedStatic.when(() -> TelemetryHelper.keyOf(any())).thenCallRealMethod();
      ITelemetryClient telemetryClient =
          TelemetryClientFactory.getInstance().getTelemetryClient(context);

      assertInstanceOf(NoopTelemetryClient.class, telemetryClient);
      assertEquals(0, TelemetryClientFactory.getInstance().telemetryClientHolders.size());
      assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());
      TelemetryClientFactory.getInstance().closeTelemetryClient(context);
    }
  }

  private void setupMocksForTelemetryClient(IDatabricksConnectionContext context) {
    TelemetryClientFactory.getInstance().closeTelemetryClient(context);
    TelemetryAuthHelper.setupAuthMocks(context, clientConfigurator);
    Map<String, String> featureFlagMap = new HashMap<>();
    featureFlagMap.put(TELEMETRY_FEATURE_FLAG_NAME, "true");
    enableFeatureFlagForTesting(context, featureFlagMap);
  }
}
