package com.databricks.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.sdk.core.DatabricksConfig;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TelemetryClientFactoryTest {
  private static final String JDBC_URL_1 =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;UserAgentEntry=MyApp";
  private static final String JDBC_URL_2 =
      "jdbc:databricks://adb-20.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/ghgjhgj;UserAgentEntry=MyApp;EnableTelemetry=1";

  @Mock DatabricksConfig databricksConfig;

  @Test
  public void testGetTelemetryClient() throws Exception {
    IDatabricksConnectionContext context1 =
        DatabricksConnectionContext.parse(JDBC_URL_1, new Properties());
    ITelemetryClient client1 =
        TelemetryClientFactory.getInstance().getTelemetryClient(context1, databricksConfig);
    ITelemetryClient unauthClient1 =
        TelemetryClientFactory.getInstance().getUnauthenticatedTelemetryClient(context1);
    assertInstanceOf(NoopTelemetryClient.class, client1);
    assertInstanceOf(NoopTelemetryClient.class, unauthClient1);
    assertEquals(0, TelemetryClientFactory.getInstance().telemetryClients.size());
    assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClients.size());
    IDatabricksConnectionContext finalContext = context1;
    assertDoesNotThrow(
        () ->
            TelemetryClientFactory.getInstance()
                .getUnauthenticatedTelemetryClient(finalContext)
                .close());
    Properties properties = new Properties();
    properties.setProperty(DatabricksJdbcUrlParams.ENABLE_TELEMETRY.getParamName(), "1");
    context1 = DatabricksConnectionContext.parse(JDBC_URL_1, properties);
    client1 = TelemetryClientFactory.getInstance().getTelemetryClient(context1, databricksConfig);
    unauthClient1 =
        TelemetryClientFactory.getInstance().getUnauthenticatedTelemetryClient(context1);
    assertInstanceOf(TelemetryClient.class, client1);
    assertInstanceOf(TelemetryClient.class, unauthClient1);
    assertNotEquals(client1, unauthClient1);
    assertEquals(1, TelemetryClientFactory.getInstance().telemetryClients.size());
    assertEquals(1, TelemetryClientFactory.getInstance().noauthTelemetryClients.size());
    IDatabricksConnectionContext context2 =
        DatabricksConnectionContext.parse(JDBC_URL_2, new Properties());
    ITelemetryClient client2 =
        TelemetryClientFactory.getInstance().getTelemetryClient(context2, databricksConfig);
    ITelemetryClient unauthClient2 =
        TelemetryClientFactory.getInstance().getUnauthenticatedTelemetryClient(context2);
    assertInstanceOf(TelemetryClient.class, client2);
    assertInstanceOf(TelemetryClient.class, unauthClient2);
    assertNotEquals(client1, client2);
    assertNotEquals(unauthClient1, unauthClient2);
    assertEquals(2, TelemetryClientFactory.getInstance().telemetryClients.size());
    assertEquals(2, TelemetryClientFactory.getInstance().noauthTelemetryClients.size());
    IDatabricksConnectionContext finalContext2 = context2;
    assertDoesNotThrow(
        () ->
            TelemetryClientFactory.getInstance()
                .getUnauthenticatedTelemetryClient(finalContext2)
                .close());

    TelemetryClientFactory.getInstance().closeTelemetryClient(context1);
    assertEquals(1, TelemetryClientFactory.getInstance().telemetryClients.size());
    assertEquals(1, TelemetryClientFactory.getInstance().noauthTelemetryClients.size());

    TelemetryClientFactory.getInstance().closeTelemetryClient(context2);
    assertEquals(0, TelemetryClientFactory.getInstance().telemetryClients.size());
    assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClients.size());
  }
}
