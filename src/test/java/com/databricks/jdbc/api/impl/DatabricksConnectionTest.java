package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.REDACTED_TOKEN;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.volume.DatabricksVolumeClientFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionInternal;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotImplementedException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
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

  private static final String WAREHOUSE_ID = "99999999";
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
      "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;UserAgentEntry=MyApp";

  private static final String JDBC_URL_WITHOUT_SCHEMA_AND_CATALOG =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:4423;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;UserAgentEntry=MyApp";
  private static final String CATALOG_SCHEMA_JDBC_URL =
      String.format(
          "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/%s;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;ConnCatalog=%s;ConnSchema=%s;logLevel=FATAL",
          SCHEMA, CATALOG, SCHEMA);
  private static final String SESSION_CONF_JDBC_URL =
      String.format(
          "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;%s",
          SESSION_CONFIGS.entrySet().stream()
              .map(e -> e.getKey() + "=" + e.getValue())
              .collect(Collectors.joining(";")));
  private static final String IGNORE_TRANSACTIONS_JDBC_URL =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;IgnoreTransactions=1";
  private static final String TRANSACTIONS_ENABLED_JDBC_URL =
      String.format(
          "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/%s;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;ConnCatalog=%s;ConnSchema=%s;logLevel=FATAL;IgnoreTransactions=0",
          SCHEMA, CATALOG, SCHEMA);
  private static final ImmutableSessionInfo IMMUTABLE_SESSION_INFO =
      ImmutableSessionInfo.builder().computeResource(warehouse).sessionId(SESSION_ID).build();
  @Mock DatabricksSdkClient databricksClient;
  @Mock DatabricksResultSet resultSet;

  private static DatabricksConnection connection;

  private static IDatabricksConnectionContext connectionContext;
  private static IDatabricksConnectionContext transactionsEnabledContext;

  @BeforeAll
  static void setup() throws DatabricksSQLException {
    connectionContext =
        DatabricksConnectionContext.parse(CATALOG_SCHEMA_JDBC_URL, new Properties());
    transactionsEnabledContext =
        DatabricksConnectionContext.parse(TRANSACTIONS_ENABLED_JDBC_URL, new Properties());
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
            eq("SET CATALOG `hive_metastore`"),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(),
            any()))
        .thenReturn(resultSet);
    when(databricksClient.executeStatement(
            eq("USE SCHEMA `default`"),
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
  public void testSetCatalogAndSchemaWithHyphenatedIdentifiers() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();

    String catalogWithHyphen = "catalog-with-hyphen";
    when(databricksClient.executeStatement(
            eq("SET CATALOG `" + catalogWithHyphen + "`"),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(),
            any()))
        .thenReturn(resultSet);
    connection.setCatalog(catalogWithHyphen);
    assertEquals(connection.getCatalog(), catalogWithHyphen);

    String schemaWithHyphen = "schema-with-hyphen";
    when(databricksClient.executeStatement(
            eq("USE SCHEMA `" + schemaWithHyphen + "`"),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(),
            any()))
        .thenReturn(resultSet);
    connection.setSchema(schemaWithHyphen);
    assertEquals(connection.getSchema(), schemaWithHyphen);

    verify(databricksClient)
        .executeStatement(
            eq("SET CATALOG `" + catalogWithHyphen + "`"),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(),
            any());
    verify(databricksClient)
        .executeStatement(
            eq("USE SCHEMA `" + schemaWithHyphen + "`"),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(),
            any());
  }

  @Test
  public void testGetSchemaAndCatalog_schemaAndCatalogNotSetViaURL() throws SQLException {
    when(databricksClient.createSession(new Warehouse(WAREHOUSE_ID), null, null, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL_WITHOUT_SCHEMA_AND_CATALOG, new Properties());
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(1)).thenReturn(DEFAULT_CATALOG);
    when(resultSet.getString(2)).thenReturn(DEFAULT_SCHEMA);
    when(databricksClient.executeStatement(
            eq("SELECT CURRENT_CATALOG(), CURRENT_SCHEMA()"),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.QUERY),
            any(),
            any()))
        .thenReturn(resultSet);
    assertEquals(connection.getCatalog(), DEFAULT_CATALOG);
    assertEquals(connection.getSchema(), DEFAULT_SCHEMA);
  }

  @Test
  public void testGetAndSetSchemaAndCatalog_invalidSchemaAndCatalog_throwsException()
      throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    when(databricksClient.executeStatement(
            eq("SET CATALOG `invalid catalog`"),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(),
            any()))
        .thenThrow(
            new DatabricksSQLException(
                "[PARSE_SYNTAX_ERROR] Syntax error at or near 'schema'",
                DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED));
    when(databricksClient.executeStatement(
            eq("USE SCHEMA `invalid schema`"),
            eq(new Warehouse(WAREHOUSE_ID)),
            eq(new HashMap<>()),
            eq(StatementType.SQL),
            any(),
            any()))
        .thenThrow(
            new DatabricksSQLException(
                "[INVALID_SET_SYNTAX] Expected format is 'SET', 'SET key', or 'SET key=value'.",
                DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED));
    assertEquals(connection.getCatalog(), CATALOG);
    assertThrows(DatabricksSQLException.class, () -> connection.setCatalog("invalid catalog"));
    assertEquals(connection.getCatalog(), CATALOG);
    assertEquals(connection.getSchema(), SCHEMA);
    assertThrows(DatabricksSQLException.class, () -> connection.setSchema("invalid schema"));
    assertEquals(connection.getSchema(), SCHEMA);
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
  public void testSetCatalogWithMultipleCatalogSupportDisabled() throws SQLException {
    // Create connection context with enableMultipleCatalogSupport=0
    String urlWithDisabledMultipleCatalog =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;enableMultipleCatalogSupport=0";
    IDatabricksConnectionContext contextWithDisabledMultipleCatalog =
        DatabricksConnectionContext.parse(urlWithDisabledMultipleCatalog, new Properties());

    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), null, "default", new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);

    DatabricksConnection connectionWithDisabledMultipleCatalog =
        new DatabricksConnection(contextWithDisabledMultipleCatalog, databricksClient);
    connectionWithDisabledMultipleCatalog.open();

    String originalCatalog = connectionWithDisabledMultipleCatalog.getSession().getCatalog();
    connectionWithDisabledMultipleCatalog.setCatalog("new_catalog");
    assertEquals(originalCatalog, connectionWithDisabledMultipleCatalog.getSession().getCatalog());
    connectionWithDisabledMultipleCatalog.close();
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
  public void testStatement() throws SQLException {
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
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.prepareCall(SQL, 10, 10));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.prepareCall(SQL, 1, 1, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> connection.prepareStatement(SQL, 1, 1, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> connection.prepareStatement(SQL, 1));
    // Test createStatement with default values succeeds
    assertDoesNotThrow(
        () -> {
          connection.createStatement(
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY,
              ResultSet.CLOSE_CURSORS_AT_COMMIT);
        });

    // Test createStatement with non-default values throws exception
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> {
          connection.createStatement(
              ResultSet.TYPE_SCROLL_INSENSITIVE,
              ResultSet.CONCUR_READ_ONLY,
              ResultSet.CLOSE_CURSORS_AT_COMMIT);
        });
    // With default IgnoreTransactions=1, savepoint methods return null (no-op)
    assertNull(connection.setSavepoint("1"));
    assertNull(connection.setSavepoint());
    assertThrows(DatabricksSQLFeatureNotImplementedException.class, connection::createClob);
    assertThrows(DatabricksSQLFeatureNotImplementedException.class, connection::createBlob);
    assertThrows(DatabricksSQLFeatureNotImplementedException.class, connection::createNClob);
    assertThrows(DatabricksSQLFeatureNotImplementedException.class, connection::createSQLXML);
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> connection.prepareStatement(SQL, new int[0]));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> connection.prepareStatement(SQL, new String[0]));
    // With default IgnoreTransactions=1, these are no-ops
    assertDoesNotThrow(() -> connection.rollback(null));
    assertDoesNotThrow(() -> connection.releaseSavepoint(null));
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
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.setShardingKeyIfValid(null, null, 0));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.setShardingKeyIfValid(null, 0));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> connection.setShardingKey(null, null));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class, () -> connection.setShardingKey(null));
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
    assertEquals(connection.getTransactionIsolation(), Connection.TRANSACTION_REPEATABLE_READ);
    connection.close();
  }

  @Test
  void testTranslationIsolation() throws SQLException {
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();
    assertDoesNotThrow(
        () -> connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> connection.setTransactionIsolation(10));
    connection.close();
  }

  @Test
  void testReadOnlyAndAbort() throws SQLException {
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

  @Test
  public void testQueryTagsInSessionConfigs() throws SQLException {
    String queryTagsJdbcUrl = JDBC_URL + ";QUERY_TAGS=team:marketing,dashboard:abc123";
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(queryTagsJdbcUrl, new Properties());

    Map<String, String> sessionConfigs = connectionContext.getSessionConfigs();
    assertTrue(sessionConfigs.containsKey("query_tags"));
    assertEquals("team:marketing,dashboard:abc123", sessionConfigs.get("query_tags"));
  }

  @Test
  public void testIsValidWithSQLValidationEnabled() throws SQLException {
    String jdbcUrlWithValidation = CATALOG_SCHEMA_JDBC_URL + ";EnableSQLValidationForIsValid=1";
    IDatabricksConnectionContext connectionContextWithValidation =
        DatabricksConnectionContext.parse(jdbcUrlWithValidation, new Properties());
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(connectionContextWithValidation, databricksClient);
    connection.open();
    DatabricksConnection spyConnection = spy(connection);
    DatabricksStatement mockStatement = mock(DatabricksStatement.class);
    doReturn(mockStatement).when(spyConnection).createStatement();
    doNothing().when(mockStatement).setQueryTimeout(anyInt());
    when(mockStatement.execute("SELECT VERSION()")).thenReturn(true);

    assertTrue(spyConnection.isValid(5));
    verify(spyConnection).createStatement();
    verify(mockStatement).setQueryTimeout(5);
    verify(mockStatement).execute("SELECT VERSION()");

    DatabricksStatement mockStatementFail = mock(DatabricksStatement.class);
    doReturn(mockStatementFail).when(spyConnection).createStatement();
    doNothing().when(mockStatementFail).setQueryTimeout(anyInt());
    when(mockStatementFail.execute("SELECT VERSION()"))
        .thenThrow(new SQLException("Connection lost"));

    assertFalse(spyConnection.isValid(5));
    verify(mockStatementFail).setQueryTimeout(5);
    verify(mockStatementFail).execute("SELECT VERSION()");

    connection.close();
  }

  @Test
  public void testIgnoreTransactionsEnabled() throws Exception {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), null, DEFAULT_SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);

    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(IGNORE_TRANSACTIONS_JDBC_URL, new Properties());
    connection = new DatabricksConnection(context, databricksClient);
    connection.open();

    assertTrue(context.getIgnoreTransactions());

    assertDoesNotThrow(() -> connection.setAutoCommit(false));
    assertDoesNotThrow(() -> connection.commit());
    assertDoesNotThrow(() -> connection.rollback());

    assertNull(connection.setSavepoint());
    assertNull(connection.setSavepoint("test"));

    connection.close();
  }

  @Test
  public void testMetricViewMetadataInSessionConfigs() throws SQLException {
    String metricViewEnabledUrl = JDBC_URL + ";EnableMetricViewMetadata=1";
    IDatabricksConnectionContext connectionContextEnabled =
        DatabricksConnectionContext.parse(metricViewEnabledUrl, new Properties());

    Map<String, String> sessionConfigsEnabled = connectionContextEnabled.getSessionConfigs();
    assertTrue(
        sessionConfigsEnabled.containsKey("spark.sql.thriftserver.metadata.metricview.enabled"));
    assertEquals(
        "true", sessionConfigsEnabled.get("spark.sql.thriftserver.metadata.metricview.enabled"));

    String metricViewDisabledUrl = JDBC_URL + ";EnableMetricViewMetadata=0";
    IDatabricksConnectionContext connectionContextDisabled =
        DatabricksConnectionContext.parse(metricViewDisabledUrl, new Properties());

    Map<String, String> sessionConfigsDisabled = connectionContextDisabled.getSessionConfigs();
    assertFalse(
        sessionConfigsDisabled.containsKey("spark.sql.thriftserver.metadata.metricview.enabled"));

    IDatabricksConnectionContext connectionContextDefault =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());

    Map<String, String> sessionConfigsDefault = connectionContextDefault.getSessionConfigs();
    assertFalse(
        sessionConfigsDefault.containsKey("spark.sql.thriftserver.metadata.metricview.enabled"));
  }

  // ==================== Transaction API Unit Tests ====================

  @Test
  public void testSetAutoCommitToFalse() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(transactionsEnabledContext, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);
    DatabricksStatement mockStatement = mock(DatabricksStatement.class);
    doReturn(mockStatement).when(spyConnection).createStatement();
    when(mockStatement.execute("SET AUTOCOMMIT = FALSE")).thenReturn(true);

    assertTrue(spyConnection.getAutoCommit());

    spyConnection.setAutoCommit(false);

    verify(mockStatement).execute("SET AUTOCOMMIT = FALSE");
    verify(mockStatement).close();

    assertFalse(spyConnection.getAutoCommit());

    spyConnection.close();
  }

  @Test
  public void testSetAutoCommitToTrue() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(transactionsEnabledContext, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);
    DatabricksStatement mockStatement = mock(DatabricksStatement.class);
    doReturn(mockStatement).when(spyConnection).createStatement();
    when(mockStatement.execute("SET AUTOCOMMIT = TRUE")).thenReturn(true);

    spyConnection.getSession().setAutoCommit(false);
    assertFalse(spyConnection.getAutoCommit());

    spyConnection.setAutoCommit(true);

    verify(mockStatement).execute("SET AUTOCOMMIT = TRUE");
    verify(mockStatement).close();

    assertTrue(spyConnection.getAutoCommit());

    spyConnection.close();
  }

  @Test
  public void testSetAutoCommitWithServerError() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(transactionsEnabledContext, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);
    DatabricksStatement mockStatement = mock(DatabricksStatement.class);
    doReturn(mockStatement).when(spyConnection).createStatement();

    SQLException serverError =
        new SQLException("AUTOCOMMIT_SET_DURING_ACTIVE_TRANSACTION", "25000", 12345); // vendor code
    when(mockStatement.execute("SET AUTOCOMMIT = FALSE")).thenThrow(serverError);

    SQLException thrown =
        assertThrows(SQLException.class, () -> spyConnection.setAutoCommit(false));

    assertTrue(thrown.getMessage().contains("AUTOCOMMIT_SET_DURING_ACTIVE_TRANSACTION"));
    assertEquals("25000", thrown.getSQLState());
    assertEquals(12345, thrown.getErrorCode()); // Vendor code preserved

    verify(mockStatement).close();

    assertTrue(spyConnection.getAutoCommit());

    spyConnection.close();
  }

  @Test
  public void testGetAutoCommitReturnsSessionValue() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();

    assertTrue(connection.getAutoCommit());

    connection.getSession().setAutoCommit(false);

    assertFalse(connection.getAutoCommit());

    connection.close();
  }

  @Test
  public void testCommitSuccess() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(transactionsEnabledContext, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);
    DatabricksStatement mockStatement = mock(DatabricksStatement.class);
    doReturn(mockStatement).when(spyConnection).createStatement();
    when(mockStatement.execute("COMMIT")).thenReturn(true);

    spyConnection.commit();

    verify(mockStatement).execute("COMMIT");
    verify(mockStatement).close();

    spyConnection.close();
  }

  @Test
  public void testCommitWithIgnoreTransactions() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), null, DEFAULT_SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);

    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(IGNORE_TRANSACTIONS_JDBC_URL, new Properties());
    connection = new DatabricksConnection(context, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);

    assertDoesNotThrow(spyConnection::commit);

    verify(spyConnection, never()).createStatement();

    spyConnection.close();
  }

  @Test
  public void testCommitWithServerError() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(transactionsEnabledContext, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);
    DatabricksStatement mockStatement = mock(DatabricksStatement.class);
    doReturn(mockStatement).when(spyConnection).createStatement();

    SQLException serverError =
        new SQLException("MULTI_STATEMENT_TRANSACTION_NO_ACTIVE_TRANSACTION", "25000", 54321);
    when(mockStatement.execute("COMMIT")).thenThrow(serverError);

    SQLException thrown = assertThrows(SQLException.class, spyConnection::commit);

    assertTrue(thrown.getMessage().contains("MULTI_STATEMENT_TRANSACTION_NO_ACTIVE_TRANSACTION"));
    assertEquals("25000", thrown.getSQLState());
    assertEquals(54321, thrown.getErrorCode()); // Vendor code preserved

    verify(mockStatement).close();

    spyConnection.close();
  }

  @Test
  public void testRollbackSuccess() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(transactionsEnabledContext, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);
    DatabricksStatement mockStatement = mock(DatabricksStatement.class);
    doReturn(mockStatement).when(spyConnection).createStatement();
    when(mockStatement.execute("ROLLBACK")).thenReturn(true);

    spyConnection.rollback();

    verify(mockStatement).execute("ROLLBACK");
    verify(mockStatement).close();

    spyConnection.close();
  }

  @Test
  public void testRollbackWithIgnoreTransactions() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), null, DEFAULT_SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);

    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(IGNORE_TRANSACTIONS_JDBC_URL, new Properties());
    connection = new DatabricksConnection(context, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);

    assertDoesNotThrow(() -> spyConnection.rollback());

    verify(spyConnection, never()).createStatement();

    spyConnection.close();
  }

  @Test
  public void testRollbackWithServerError() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(transactionsEnabledContext, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);
    DatabricksStatement mockStatement = mock(DatabricksStatement.class);
    doReturn(mockStatement).when(spyConnection).createStatement();

    SQLException serverError = new SQLException("Unexpected rollback error", "HY000", 99999);
    when(mockStatement.execute("ROLLBACK")).thenThrow(serverError);

    SQLException thrown = assertThrows(SQLException.class, spyConnection::rollback);

    assertTrue(thrown.getMessage().contains("Unexpected rollback error"));
    assertEquals("HY000", thrown.getSQLState());
    assertEquals(99999, thrown.getErrorCode()); // Vendor code preserved

    verify(mockStatement).close();

    spyConnection.close();
  }

  @Test
  public void testTransactionMethodsPreserveExceptionChain() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(transactionsEnabledContext, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);
    DatabricksStatement mockStatement = mock(DatabricksStatement.class);
    doReturn(mockStatement).when(spyConnection).createStatement();

    SQLException rootCause = new SQLException("Root cause", "XX000", 11111);
    SQLException serverError = new SQLException("Wrapper error", "25000", 22222, rootCause);
    when(mockStatement.execute("SET AUTOCOMMIT = FALSE")).thenThrow(serverError);

    SQLException thrown =
        assertThrows(SQLException.class, () -> spyConnection.setAutoCommit(false));

    assertNotNull(thrown.getCause());
    assertEquals("Wrapper error", thrown.getCause().getMessage());
    assertEquals(rootCause, thrown.getCause().getCause());

    spyConnection.close();
  }

  // ==================== FetchAutoCommitFromServer Tests ====================

  @Test
  public void testGetAutoCommitWithFetchFromServerEnabled_ReturnsTrue() throws SQLException {
    // Create connection context with FetchAutoCommitFromServer=1
    String urlWithFetch = CATALOG_SCHEMA_JDBC_URL + ";FetchAutoCommitFromServer=1";
    IDatabricksConnectionContext contextWithFetch =
        DatabricksConnectionContext.parse(urlWithFetch, new Properties());

    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(contextWithFetch, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);
    DatabricksStatement mockStatement = mock(DatabricksStatement.class);
    ResultSet mockResultSet = mock(ResultSet.class);

    doReturn(mockStatement).when(spyConnection).createStatement();
    when(mockStatement.executeQuery("SET AUTOCOMMIT")).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(1)).thenReturn("true"); // Server returns "true"

    // Call getAutoCommit - should query server
    boolean result = spyConnection.getAutoCommit();

    // Verify server was queried
    verify(mockStatement).executeQuery("SET AUTOCOMMIT");
    verify(mockResultSet).getString(1);
    verify(mockStatement).close();

    // Verify result is true
    assertTrue(result);

    // Verify cache was updated
    assertTrue(spyConnection.getSession().getAutoCommit());

    spyConnection.close();
  }

  @Test
  public void testGetAutoCommitWithFetchFromServerEnabled_ReturnsFalse() throws SQLException {
    // Create connection context with FetchAutoCommitFromServer=1
    String urlWithFetch = CATALOG_SCHEMA_JDBC_URL + ";FetchAutoCommitFromServer=1";
    IDatabricksConnectionContext contextWithFetch =
        DatabricksConnectionContext.parse(urlWithFetch, new Properties());

    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(contextWithFetch, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);
    DatabricksStatement mockStatement = mock(DatabricksStatement.class);
    ResultSet mockResultSet = mock(ResultSet.class);

    doReturn(mockStatement).when(spyConnection).createStatement();
    when(mockStatement.executeQuery("SET AUTOCOMMIT")).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(1)).thenReturn("false"); // Server returns "false"

    // Call getAutoCommit - should query server
    boolean result = spyConnection.getAutoCommit();

    // Verify server was queried
    verify(mockStatement).executeQuery("SET AUTOCOMMIT");
    verify(mockResultSet).getString(1);
    verify(mockStatement).close();

    // Verify result is false
    assertFalse(result);

    // Verify cache was updated
    assertFalse(spyConnection.getSession().getAutoCommit());

    spyConnection.close();
  }

  @Test
  public void testGetAutoCommitWithFetchFromServerEnabled_ServerQueryFails() throws SQLException {
    // Create connection context with FetchAutoCommitFromServer=1
    String urlWithFetch = CATALOG_SCHEMA_JDBC_URL + ";FetchAutoCommitFromServer=1";
    IDatabricksConnectionContext contextWithFetch =
        DatabricksConnectionContext.parse(urlWithFetch, new Properties());

    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(contextWithFetch, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);
    DatabricksStatement mockStatement = mock(DatabricksStatement.class);

    doReturn(mockStatement).when(spyConnection).createStatement();
    SQLException serverError = new SQLException("Server error during SET AUTOCOMMIT query");
    when(mockStatement.executeQuery("SET AUTOCOMMIT")).thenThrow(serverError);

    // Call getAutoCommit - should throw exception
    SQLException thrown = assertThrows(DatabricksSQLException.class, spyConnection::getAutoCommit);

    // Verify error message
    assertTrue(
        thrown.getMessage().contains("Failed to fetch autoCommit state from server"),
        "Exception message should indicate fetch failure");

    // Verify statement was closed even after exception
    verify(mockStatement).close();

    spyConnection.close();
  }

  // ==================== SetAutoCommit Optimization Tests ====================

  @Test
  public void testSetAutoCommitSkipsServerCallWhenAlreadyTrue() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);

    assertTrue(spyConnection.getAutoCommit());

    spyConnection.setAutoCommit(true);

    // Verify createStatement was never called (no server round-trip)
    verify(spyConnection, never()).createStatement();

    spyConnection.close();
  }

  @Test
  public void testSetAutoCommitSkipsServerCallWhenAlreadyFalse() throws SQLException {
    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(connectionContext, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);

    spyConnection.getSession().setAutoCommit(false);
    assertFalse(spyConnection.getAutoCommit());

    spyConnection.setAutoCommit(false);

    // Verify createStatement was never called
    verify(spyConnection, never()).createStatement();

    spyConnection.close();
  }

  @Test
  public void testSetAutoCommitDoesNotSkipWithFetchFromServerEnabled() throws SQLException {
    // Create connection with FetchAutoCommitFromServer=1 and IgnoreTransactions=0
    String urlWithFetch =
        CATALOG_SCHEMA_JDBC_URL + ";FetchAutoCommitFromServer=1;IgnoreTransactions=0";
    IDatabricksConnectionContext contextWithFetch =
        DatabricksConnectionContext.parse(urlWithFetch, new Properties());

    when(databricksClient.createSession(
            new Warehouse(WAREHOUSE_ID), CATALOG, SCHEMA, new HashMap<>()))
        .thenReturn(IMMUTABLE_SESSION_INFO);
    connection = new DatabricksConnection(contextWithFetch, databricksClient);
    connection.open();

    DatabricksConnection spyConnection = spy(connection);
    DatabricksStatement mockStatement = mock(DatabricksStatement.class);
    doReturn(mockStatement).when(spyConnection).createStatement();
    when(mockStatement.execute("SET AUTOCOMMIT = TRUE")).thenReturn(true);

    // Even though autoCommit is already true, server should be called
    spyConnection.setAutoCommit(true);

    // Verify server was called
    verify(mockStatement).execute("SET AUTOCOMMIT = TRUE");

    spyConnection.close();
  }
}
