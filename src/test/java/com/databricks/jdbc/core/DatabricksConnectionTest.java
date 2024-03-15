package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.core.types.Warehouse;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.UserAgent;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksConnectionTest {

  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final ComputeResource warehouse = new Warehouse(WAREHOUSE_ID);
  private static final String CATALOG = "field_demos";
  private static final String SCHEMA = "ossjdbc";
  static final String DEFAULT_SCHEMA = "default";
  static final String DEFAULT_CATALOG = "SPARK";
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
  private static final ImmutableSessionInfo IMMUTABLE_SESSION_INFO =
      ImmutableSessionInfo.builder().computeResource(warehouse).sessionId(SESSION_ID).build();
  @Mock DatabricksSdkClient databricksClient;

  @Test
  public void testConnection() throws Exception {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), DEFAULT_CATALOG, DEFAULT_SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, databricksClient);
    assertFalse(connection.isClosed());
    assertEquals(connection.getSession().getSessionId(), SESSION_ID);
    String userAgent = UserAgent.asString();
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/0.0.0"));
    assertTrue(userAgent.contains("Java/SQLExecHttpClient/HC MyApp"));

    // close the connection
    connection.close();
    assertTrue(connection.isClosed());
    verify(databricksClient).deleteSession(SESSION_ID, warehouse);
  }

  @Test
  public void testCatalogSettingInConnection() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(CATALOG_SCHEMA_JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, databricksClient);
    assertFalse(connection.isClosed());
    assertEquals(connection.getSession().getCatalog(), CATALOG);
    assertEquals(connection.getSession().getSchema(), SCHEMA);
  }

  @Test
  public void testConfInConnection() throws SQLException {
    Map<String, String> lowercaseSessionConfigs =
        SESSION_CONFIGS.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toLowerCase(), Map.Entry::getValue));
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), DEFAULT_CATALOG, DEFAULT_SCHEMA, lowercaseSessionConfigs))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(SESSION_CONF_JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, databricksClient);
    assertFalse(connection.isClosed());
    assertEquals(connection.getSession().getSessionConfigs(), lowercaseSessionConfigs);
  }

  @Test
  public void testStatement() throws Exception {
    ImmutableSessionInfo session =
        ImmutableSessionInfo.builder().computeResource(warehouse).sessionId(SESSION_ID).build();
    when(databricksClient.createSession(eq(new Warehouse(WAREHOUSE_ID)), any(), any(), any()))
        .thenReturn(session);
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

  @Test
  public void testSetClientInfo() throws SQLException {
    Properties properties = new Properties();
    properties.put("ENABLE_PHOTON", "TRUE");
    properties.put("TIMEZONE", "UTC");
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    ImmutableSessionInfo session =
        ImmutableSessionInfo.builder().computeResource(warehouse).sessionId(SESSION_ID).build();
    when(databricksClient.createSession(
            warehouse, DEFAULT_CATALOG, DEFAULT_SCHEMA, new HashMap<>()))
        .thenReturn(session);
    DatabricksConnection connection =
        Mockito.spy(new DatabricksConnection(connectionContext, databricksClient));
    DatabricksStatement statement = Mockito.spy(new DatabricksStatement(connection));
    Mockito.doReturn(statement).when(connection).createStatement();
    Mockito.doReturn(true).when(statement).execute("SET ENABLE_PHOTON = TRUE");
    Mockito.doReturn(true).when(statement).execute("SET TIMEZONE = UTC");
    Mockito.doThrow(
            new SQLException(
                "Unable to set property",
                new SQLException("Configuration RANDOM_CONF is not available")))
        .when(statement)
        .execute("SET RANDOM_CONF = UNLIMITED");
    connection.setClientInfo(properties);
    Properties clientInfoProperties = connection.getClientInfo();
    // Check valid session confs are set
    assertEquals(connection.getClientInfo("ENABLE_PHOTON"), "TRUE");
    assertEquals(connection.getClientInfo("TIMEZONE"), "UTC");
    assertEquals(clientInfoProperties.get("ENABLE_PHOTON"), "TRUE");
    assertEquals(clientInfoProperties.get("TIMEZONE"), "UTC");
    // Check conf not supplied returns default value
    assertEquals(connection.getClientInfo("MAX_FILE_PARTITION_BYTES"), "128m");
    assertEquals(clientInfoProperties.get("MAX_FILE_PARTITION_BYTES"), "128m");
    // Checks for unknown conf
    assertThrows(
        SQLClientInfoException.class, () -> connection.setClientInfo("RANDOM_CONF", "UNLIMITED"));
    assertNull(connection.getClientInfo("RANDOM_CONF"));
    assertNull(clientInfoProperties.get("RANDOM_CONF"));
  }
}
