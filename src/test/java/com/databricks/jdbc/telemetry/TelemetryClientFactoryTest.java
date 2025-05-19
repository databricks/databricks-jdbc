package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.common.FeatureFlagTestUtil.enableFeatureFlagForTesting;
import static com.databricks.jdbc.telemetry.TelemetryHelper.TELEMETRY_FEATURE_FLAG_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.auth.AuthTestHelper;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.sdk.core.DatabricksConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TelemetryClientFactoryTest {
  private static final String JDBC_URL_1 =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;UserAgentEntry=MyApp;";
  private static final String JDBC_URL_2 =
      "jdbc:databricks://adb-20.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/ghgjhgj;UserAgentEntry=MyApp;EnableTelemetry=1";

  @Mock DatabricksConfig databricksConfig;
  @Mock ClientConfigurator clientConfigurator;

  @Test
  public void testGetNoOpTelemetryClient() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL_1, new Properties());
    ITelemetryClient telemetryClient =
        TelemetryClientFactory.getInstance().getTelemetryClient(context);
    assertInstanceOf(NoopTelemetryClient.class, telemetryClient);
    assertEquals(0, TelemetryClientFactory.getInstance().telemetryClients.size());
    assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClients.size());
    TelemetryClientFactory.getInstance().closeTelemetryClient(context);
    assertEquals(0, TelemetryClientFactory.getInstance().telemetryClients.size());
    assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClients.size());
  }

  @Test
  public void testGetUnAuthenticatedTelemetryClient() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(DatabricksJdbcUrlParams.ENABLE_TELEMETRY.getParamName(), "1");
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL_1, properties);
    setupMocksForTelemetryClient(context, null);
    ITelemetryClient telemetryClient =
        TelemetryClientFactory.getInstance().getTelemetryClient(context);
    assertInstanceOf(TelemetryClient.class, telemetryClient);
    assertEquals(0, TelemetryClientFactory.getInstance().telemetryClients.size());
    assertEquals(1, TelemetryClientFactory.getInstance().noauthTelemetryClients.size());
    TelemetryClientFactory.getInstance().closeTelemetryClient(context);
    assertEquals(0, TelemetryClientFactory.getInstance().telemetryClients.size());
    assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClients.size());
  }

  @Test
  public void testGetAuthenticatedTelemetryClient() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL_2, new Properties());
    ITelemetryClient telemetryClient =
        TelemetryClientFactory.getInstance().getTelemetryClient(context);
    setupMocksForTelemetryClient(context, databricksConfig);
    assertInstanceOf(TelemetryClient.class, telemetryClient);
    assertEquals(1, TelemetryClientFactory.getInstance().telemetryClients.size());
    assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClients.size());
    TelemetryClientFactory.getInstance().closeTelemetryClient(context);
    assertEquals(0, TelemetryClientFactory.getInstance().telemetryClients.size());
    assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClients.size());
  }

  private void setupMocksForTelemetryClient(
      IDatabricksConnectionContext context, DatabricksConfig databricksConfig) {
    Map<String, String> featureFlagMap = new HashMap<>();
    if (databricksConfig != null) {
      when(databricksConfig.authenticate()).thenReturn(Collections.emptyMap());
    }
    featureFlagMap.put(TELEMETRY_FEATURE_FLAG_NAME, "true");
    enableFeatureFlagForTesting(context, featureFlagMap);
    AuthTestHelper.setupAuthMocks(context, clientConfigurator, databricksConfig);
  }
}
