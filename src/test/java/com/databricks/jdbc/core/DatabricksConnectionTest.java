package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.UserAgent;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksConnectionTest {

  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final String CATALOG = "field_demos";
  private static final String SCHEMA = "ossjdbc";
  private static final String SESSION_ID = "session_id";
  private static final Map<String, String> SESSION_CONFIGS =
      Map.of("ANSI_MODE", "TRUE", "TIMEZONE", "UTC", "MAX_FILE_PARTITION_BYTES", "64m");
  private static final String JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;UserAgentEntry=MyApp";
  private static final String CATALOG_SCHEMA_JDBC_URL =
      String.format(
          "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;ConnCatalog=%s;ConnSchema=%s",
          CATALOG, SCHEMA);
  private static final String SESSION_CONF_JDBC_URL =
      String.format(
          "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;%s",
          SESSION_CONFIGS.entrySet().stream()
              .map(e -> e.getKey() + "=" + e.getValue())
              .collect(Collectors.joining(";")));

  @Mock DatabricksSdkClient databricksClient;

  @Test
  public void testConnection() throws Exception {
    ImmutableSessionInfo session =
        ImmutableSessionInfo.builder().warehouseId(WAREHOUSE_ID).sessionId(SESSION_ID).build();

    when(databricksClient.createSession(WAREHOUSE_ID, null, null, new HashMap<>()))
        .thenReturn(session);

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, databricksClient);

    assertFalse(connection.isClosed());
    assertEquals(connection.getSession().getSessionId(), SESSION_ID);
    String userAgent = UserAgent.asString();
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/0.0.0"));
    assertTrue(userAgent.contains("Java/SQLExecHttpClient/HC MyApp"));

    when(databricksClient.createSession(WAREHOUSE_ID, CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(session);
    connectionContext =
        DatabricksConnectionContext.parse(CATALOG_SCHEMA_JDBC_URL, new Properties());
    connection = new DatabricksConnection(connectionContext, databricksClient);
    assertFalse(connection.isClosed());
    assertEquals(connection.getSession().getCatalog(), CATALOG);
    assertEquals(connection.getSession().getSchema(), SCHEMA);

    Map<String, String> lowercaseSessionConfigs =
        SESSION_CONFIGS.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toLowerCase(), Map.Entry::getValue));
    when(databricksClient.createSession(WAREHOUSE_ID, null, null, lowercaseSessionConfigs))
        .thenReturn(session);
    connectionContext = DatabricksConnectionContext.parse(SESSION_CONF_JDBC_URL, new Properties());
    connection = new DatabricksConnection(connectionContext, databricksClient);
    assertFalse(connection.isClosed());
    assertEquals(connection.getSession().getSessionConfigs(), lowercaseSessionConfigs);
  }

  @Test
  public void testStatement() throws Exception {
    ImmutableSessionInfo session =
        ImmutableSessionInfo.builder().warehouseId(WAREHOUSE_ID).sessionId(SESSION_ID).build();

    when(databricksClient.createSession(eq(WAREHOUSE_ID), any(), any(), any())).thenReturn(session);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, databricksClient);

    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> {
          connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        });

    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> {
          connection.prepareStatement(
              "sql", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        });

    assertDoesNotThrow(
        () -> {
          connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        });

    assertDoesNotThrow(
        () -> {
          connection.prepareStatement(
              "sql", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        });
  }
}
