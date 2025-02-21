package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.REDACTED_TOKEN;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.volume.DatabricksVolumeClientFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionInternal;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotImplementedException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksConnectionTest {

  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final IDatabricksComputeResource warehouse = new Warehouse(WAREHOUSE_ID);
  private static final String CATALOG = "field_demos";
  private static final String SQL = "select 1";
  private static final String SCHEMA = "ossjdbc";
  static final String DEFAULT_SCHEMA = "default";
  static final String DEFAULT_CATALOG = "hive_metastore";
  private static final String SESSION_ID = "session_id";
  private static final Map<String, String> SESSION_CONFIGS =
      Map.of("ANSI_MODE", "TRUE", "TIMEZONE", "UTC", "MAX_FILE_PARTITION_BYTES", "64m");
  private static final String JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;UserAgentEntry=MyApp";
  private static final String CATALOG_SCHEMA_JDBC_URL =
      String.format(
          "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/%s;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;ConnCatalog=%s;ConnSchema=%s;logLevel=FATAL",
          SCHEMA, CATALOG, SCHEMA);
  private static final String SESSION_CONF_JDBC_URL =
      String.format(
          "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;%s",
          SESSION_CONFIGS.entrySet().stream()
              .map(e -> e.getKey() + "=" + e.getValue())
              .collect(Collectors.joining(";")));
  private static final ImmutableSessionInfo IMMUTABLE_SESSION_INFO =
      ImmutableSessionInfo.builder().computeResource(warehouse).sessionId(SESSION_ID).build();
  @Mock DatabricksSdkClient databricksClient;
  @Mock DatabricksResultSet resultSet;

  private static DatabricksConnection connection;

  private static IDatabricksConnectionContext connectionContext;

  @BeforeAll
  static void setup() throws DatabricksSQLException {
    connectionContext =
        DatabricksConnectionContext.parse(CATALOG_SCHEMA_JDBC_URL, new Properties());
  }

  @Test
  public void testConnection() throws Exception {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    assertEquals(DatabricksThreadContextHolder.getConnectionContext(), connectionContext);
    assertFalse(connection.isClosed());
    assertEquals(connection.getSession().getSessionId(), SESSION_ID);
    // close the connection
    connection.close();
    assertNull(DatabricksThreadContextHolder.getConnectionContext());
    assertTrue(connection.isClosed());
    assertEquals(connection.getConnection(), connection);
  }

  @Test
  public void testGetAndSetSchemaAndCatalog() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    when(databricksClient.executeStatement(
            eq("SET CATALOG hive_metastore"),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(),
            any()))
        .thenReturn(resultSet);
    when(databricksClient.executeStatement(
            eq("USE SCHEMA default"),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(),
            any()))
        .thenReturn(resultSet);
    assertEquals(connection.getCatalog(), CATALOG);
    connection.setCatalog(DEFAULT_CATALOG);
    assertEquals(connection.getCatalog(), DEFAULT_CATALOG);
    assertEquals(connection.getSchema(), SCHEMA);
    connection.setSchema(DEFAULT_SCHEMA);
    assertEquals(connection.getSchema(), DEFAULT_SCHEMA);
  }

  @Test
  public void testCatalogSettingInConnection() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    assertFalse(connection.isClosed());
    assertEquals(connection.getSession().getCatalog(), CATALOG);
    assertEquals(connection.getSession().getSchema(), SCHEMA);
  }

  @Test
  public void testClosedConnection() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    assertTrue(connection.isValid(1));
    connection.close();
    assertFalse(connection.isValid(1));
    assertThrows(DatabricksSQLException.class, connection::isReadOnly);
  }

  @Test
  public void testConfInConnection() throws SQLException {
    Map<String, String> lowercaseSessionConfigs =
        SESSION_CONFIGS.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toLowerCase(), Map.Entry::getValue));
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), null, "default", lowercaseSessionConfigs))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(SESSION_CONF_JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    assertFalse(connection.isClosed());
    assertEquals(connection.getSession().getSessionConfigs(), lowercaseSessionConfigs);
  }

  @Test
  public void testGetUCVolumeClient() throws SQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(SESSION_CONF_JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    DatabricksVolumeClientFactory volumeClientFactory =
        new DatabricksVolumeClientFactory(); // test constructor
    assertNotNull(volumeClientFactory.getVolumeClient(connection));
    assertNotNull(volumeClientFactory.getVolumeClient(connectionContext));
  }

  @Test
  public void testStatement() throws DatabricksSQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    assertEquals(DatabricksThreadContextHolder.getConnectionContext(), connectionContext);
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
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    assertEquals(DatabricksThreadContextHolder.getConnectionContext(), connectionContext);
    Properties properties = new Properties();
    properties.put("ENABLE_PHOTON", "TRUE");
    properties.put("TIMEZONE", "UTC");
    properties.put("use_cached_result", "false");
    properties.put("StagingAllowedLocalPaths", "/tmp");
    properties.put("Auth_AccessToken", "token");
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    ImmutableSessionInfo session =
        ImmutableSessionInfo.builder().computeResource(warehouse).sessionId(SESSION_ID).build();
    when(databricksClient.createSession(warehouse, null, "default", new HashMap<>()))
        .thenReturn(session);
    DatabricksConnection connection =
        Mockito.spy(new DatabricksConnection(connectionContext, databricksClient));
    connection.open();
    DatabricksStatement statement = Mockito.spy(new DatabricksStatement(connection));
    Mockito.doReturn(statement).when(connection).createStatement();
    Mockito.doReturn(true).when(statement).execute("SET ENABLE_PHOTON = TRUE");
    Mockito.doReturn(true).when(statement).execute("SET TIMEZONE = UTC");
    Mockito.doReturn(true).when(statement).execute("SET use_cached_result = false");

    connection.setClientInfo(properties);
    Properties clientInfoProperties = connection.getClientInfo();
    System.out.println("client info properties: " + clientInfoProperties);
    assertEquals(
        clientInfoProperties.size(),
        ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.size()
            + 2); // no duplicate values. +2 for client confs
    // Check valid session confs are set
    assertEquals(connection.getClientInfo("ENABLE_PHOTON"), "TRUE");
    assertEquals(connection.getClientInfo("timezone"), "UTC");
    assertEquals(clientInfoProperties.get("enable_photon"), "TRUE");
    assertEquals(clientInfoProperties.get("timezone"), "UTC");
    // Check conf not supplied returns default value
    assertEquals(connection.getClientInfo("MAX_FILE_PARTITION_BYTES"), "128m");
    assertEquals(clientInfoProperties.get("max_file_partition_bytes"), "128m");
    assertEquals(clientInfoProperties.get("use_cached_result"), "false");
    assertNull(clientInfoProperties.get("USE_CACHED_RESULT"));

    assertEquals(connection.getClientInfo("STAGINGALLOWEDLOCALPATHS"), "/tmp"); // case insensitive
    assertEquals(clientInfoProperties.get("stagingallowedlocalpaths"), "/tmp");

    assertEquals(connection.getClientInfo("Auth_ACCESSTOKEN"), REDACTED_TOKEN);
    assertEquals(clientInfoProperties.get("auth_accesstoken"), REDACTED_TOKEN);
    // Checks for unknown conf
    assertThrows(
        SQLClientInfoException.class, () -> connection.setClientInfo("RANDOM_CONF", "UNLIMITED"));
    assertNull(connection.getClientInfo("RANDOM_CONF"));
    assertNull(clientInfoProperties.get("RANDOM_CONF"));
  }

  @Test
  void testUnsupportedOperations() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class, () -> connection.prepareCall(SQL));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> connection.nativeSQL(SQL));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> connection.setAutoCommit(false));
    assertThrows(DatabricksSQLFeatureNotImplementedException.class, connection::commit);
    assertThrows(DatabricksSQLFeatureNotImplementedException.class, connection::rollback);
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.prepareCall(SQL, 10, 10));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.prepareCall(SQL, 1, 1, 1));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.prepareStatement(SQL, 1, 1, 1));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.prepareStatement(SQL, 1));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.prepareStatement(SQL, 1));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.createStatement(1, 1, 1));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class, () -> connection.setSavepoint("1"));
    assertThrows(DatabricksSQLFeatureNotImplementedException.class, connection::setSavepoint);
    assertThrows(DatabricksSQLFeatureNotImplementedException.class, connection::createClob);
    assertThrows(DatabricksSQLFeatureNotImplementedException.class, connection::createBlob);
    assertThrows(DatabricksSQLFeatureNotImplementedException.class, connection::createNClob);
    assertThrows(DatabricksSQLFeatureNotImplementedException.class, connection::createSQLXML);
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.prepareStatement(SQL, new int[0]));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.prepareStatement(SQL, new String[0]));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class, () -> connection.rollback(null));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class, () -> connection.releaseSavepoint(null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> connection.setNetworkTimeout(null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> connection.getNetworkTimeout());
    assertInstanceOf(
        IDatabricksConnectionInternal.class,
        connection.unwrap(IDatabricksConnectionInternal.class));
    assertTrue(connection.isWrapperFor(IDatabricksConnectionInternal.class));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.createArrayOf(null, null));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.createStruct(null, null));
  }

  @Test
  void testCommonMethods() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    assertFalse(connection.isReadOnly());
    assertNull(connection.getWarnings());
    connection.clearWarnings();
    assertDoesNotThrow(() -> connection.createStatement());
    assertNull(connection.getWarnings());
    assertTrue(connection.getAutoCommit());
    assertEquals(connection.getTransactionIsolation(), Connection.TRANSACTION_READ_UNCOMMITTED);
    connection.close();
  }

  @Test
  void testTranslationIsolation() throws DatabricksSQLException {
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    assertDoesNotThrow(
        () -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> connection.setTransactionIsolation(10));
    connection.close();
  }

  @Test
  void testReadOnlyAndAbort() throws DatabricksSQLException {
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    assertDoesNotThrow(() -> connection.setReadOnly(false));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> connection.setReadOnly(true));
    ExecutorService executorService = Executors.newFixedThreadPool(1);
    assertDoesNotThrow(() -> connection.abort(executorService));
    connection.close();
  }

  @Test
  void testTypeMap() {
    assertEquals(new HashMap<>(), connection.getTypeMap());
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> connection.setTypeMap(Collections.emptyMap()));
  }

  @Test
  void testHoldability() throws SQLException {
    assertEquals(2, connection.getHoldability());
    assertDoesNotThrow(() -> connection.setHoldability(2));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> connection.setHoldability(3));
  }
}
