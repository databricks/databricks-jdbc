package com.databricks.jdbc.integration;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;
import static com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader.TEST_CATALOG;
import static com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader.TEST_SCHEMA;
import static com.databricks.jdbc.integration.fakeservice.FakeServiceExtension.TARGET_URI_PROP_SUFFIX;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.common.DatabricksJdbcConstants.FakeServiceType;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Utility class to support integration tests * */
public class IntegrationTestUtil {

  /** Get the host of the embedded web server of fake service to be used in the tests. */
  public static String getFakeServiceHost() {
    // Target base URL of the fake service type
    FakeServiceType databricksFakeServiceType =
        FakeServiceConfigLoader.shouldUseThriftClient
            ? FakeServiceType.SQL_GATEWAY
            : FakeServiceType.SQL_EXEC;
    String serviceURI =
        System.getProperty(databricksFakeServiceType.name().toLowerCase() + TARGET_URI_PROP_SUFFIX);

    URI fakeServiceURI;
    try {
      // Fake service URL for the base URL
      fakeServiceURI = new URI(System.getProperty(serviceURI + FAKE_SERVICE_URI_PROP_SUFFIX));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    return fakeServiceURI.getAuthority();
  }

  public static String getFakeServiceM2MUrl() {
    // SSL is disabled as embedded web server of fake service uses HTTP protocol.
    // Note that in RECORD mode, the web server interacts with production services over HTTPS.
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=0;authmech=11;auth_flow=1;httpPath=%s;usethriftclient=0;";
    return String.format(template, getFakeServiceHost(), getM2MHTTPPath());
  }

  public static String getFakeServiceM2MPrivateKeyCredentialsUrl() {
    // SSL is disabled as embedded web server of fake service uses HTTP protocol.
    // Note that in RECORD mode, the web server interacts with production services over HTTPS.
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=0;authmech=11;auth_flow=1;httpPath=%s;UseJWTAssertion=1;usethriftclient=0;";
    return String.format(template, getFakeServiceHost(), getM2MPrivateKeyCredentialsHTTPPath());
  }

  public static String getJWTTokenEndpointHost() {
    String tokenEndpoint = System.getenv("DATABRICKS_JDBC_M2M_PRIVATE_KEY_TOKEN_ENDPOINT");
    return tokenEndpoint.replaceAll("^(https?://[^/]+).*", "$1");
  }

  public static String getFakeServiceJDBCUrl() {
    // The fake service client has SSL disabled, but SSL is enabled for its communication with
    // production services.
    String jdbcUrlTemplate = "jdbc:databricks://%s/default;ssl=0;AuthMech=3;httpPath=%s";
    return String.format(
        jdbcUrlTemplate,
        getFakeServiceHost(),
        FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.HTTP_PATH.getParamName()));
  }

  public static String getFakeServiceHTTPPath() {
    return FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.HTTP_PATH.getParamName());
  }

  public static String getDatabricksHost() {
    // includes port
    return System.getenv("DATABRICKS_HOST");
  }

  public static String getDatabricksBenchfoodHost() {
    // includes port
    return System.getenv("DATABRICKS_BENCHFOOD_HOST");
  }

  public static String getM2MHost() {
    return System.getenv("DATABRICKS_JDBC_M2M_HOST");
  }

  public static String getM2MPrivateKeyCredentialsHost() {
    return System.getenv("DATABRICKS_JDBC_M2M_PRIVATE_KEY_CREDENTIALS_HOST");
  }

  public static String getM2MHTTPPath() {
    return System.getenv("DATABRICKS_JDBC_M2M_HTTP_PATH");
  }

  public static String getM2MPrivateKeyCredentialsHTTPPath() {
    return System.getenv("DATABRICKS_JDBC_M2M_PRIVATE_KEY_CREDENTIALS_HTTP_PATH");
  }

  public static String getJdbcM2MUrl() {
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=0;authmech=11;auth_flow=1;httpPath=%s";
    return String.format(template, getM2MHost(), getM2MHTTPPath());
  }

  public static Connection getValidM2MConnection() throws SQLException {
    return DriverManager.getConnection(getJdbcM2MUrl(), createM2MConnectionProperties());
  }

  public static Properties createM2MConnectionProperties() {
    Properties connProps = new Properties();
    connProps.put("OAuth2ClientId", System.getenv("DATABRICKS_JDBC_M2M_CLIENT_ID"));
    connProps.put("OAuth2Secret", System.getenv("DATABRICKS_JDBC_M2M_CLIENT_SECRET"));
    return connProps;
  }

  public static String getDatabricksDogfoodHost() {
    return System.getenv("DATABRICKS_DOGFOOD_HOST");
  }

  public static String getDatabricksToken() {
    return System.getenv("DATABRICKS_TOKEN");
  }

  public static String getDatabricksDogfoodToken() {
    return System.getenv("DATABRICKS_DOGFOOD_TOKEN");
  }

  public static String getDatabricksBenchfoodToken() {
    return System.getenv("DATABRICKS_BENCHFOOD_TOKEN");
  }

  public static String getDatabricksHTTPPath() {
    return System.getenv("DATABRICKS_HTTP_PATH");
  }

  public static String getDatabricksBenchfoodHTTPPath() {
    return System.getenv("DATABRICKS_BENCHFOOD_HTTP_PATH");
  }

  public static String getDatabricksDogfoodHTTPPath() {
    return System.getenv("DATABRICKS_DOGFOOD_HTTP_PATH");
  }

  public static String getDatabricksCatalog() {
    return DriverUtil.isRunningAgainstFake()
        ? FakeServiceConfigLoader.getProperty(TEST_CATALOG)
        : System.getenv("DATABRICKS_CATALOG");
  }

  public static String getDatabricksSchema() {
    return DriverUtil.isRunningAgainstFake()
        ? FakeServiceConfigLoader.getProperty(TEST_SCHEMA)
        : System.getenv("DATABRICKS_SCHEMA");
  }

  public static String getDatabricksUser() {
    return System.getenv("DATABRICKS_USER");
  }

  public static String getPreSignedUrlHost() {
    return System.getenv("DATABRICKS_PRE_SIGNED_URL_HOST");
  }

  public static Connection getValidJDBCConnection() throws SQLException {
    Properties connectionProperties = new Properties();
    connectionProperties.put(DatabricksJdbcUrlParams.USER.getParamName(), getDatabricksUser());
    connectionProperties.put(DatabricksJdbcUrlParams.PASSWORD.getParamName(), getDatabricksToken());

    if (DriverUtil.isRunningAgainstFake()) {
      connectionProperties.put(
          DatabricksJdbcUrlParams.CONN_CATALOG.getParamName(),
          FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CONN_CATALOG.getParamName()));
      connectionProperties.put(
          DatabricksJdbcUrlParams.CONN_SCHEMA.getParamName(),
          FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CONN_SCHEMA.getParamName()));
      connectionProperties.put(
          DatabricksJdbcUrlParams.USE_THRIFT_CLIENT.getParamName(),
          FakeServiceConfigLoader.getProperty(
              DatabricksJdbcUrlParams.USE_THRIFT_CLIENT.getParamName()));

      return DriverManager.getConnection(getFakeServiceJDBCUrl(), connectionProperties);
    }

    return DriverManager.getConnection(getJDBCUrl(), connectionProperties);
  }

  public static Connection getValidJDBCConnection(List<List<String>> extraArgs)
      throws SQLException {
    String jdbcUrl = getJDBCUrl();
    for (List<String> args : extraArgs) {
      jdbcUrl += ";" + args.get(0) + "=" + args.get(1);
    }
    return DriverManager.getConnection(jdbcUrl, getDatabricksUser(), getDatabricksToken());
  }

  public static Connection getValidJDBCConnection(Properties connectionProperties)
      throws SQLException {
    connectionProperties.put(DatabricksJdbcUrlParams.USER.getParamName(), getDatabricksUser());
    connectionProperties.put(DatabricksJdbcUrlParams.PASSWORD.getParamName(), getDatabricksToken());

    if (DriverUtil.isRunningAgainstFake()) {
      connectionProperties.put(
          DatabricksJdbcUrlParams.CONN_CATALOG.getParamName(),
          FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CONN_CATALOG.getParamName()));
      connectionProperties.put(
          DatabricksJdbcUrlParams.CONN_SCHEMA.getParamName(),
          FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CONN_SCHEMA.getParamName()));
      connectionProperties.put(
          DatabricksJdbcUrlParams.USE_THRIFT_CLIENT.getParamName(),
          FakeServiceConfigLoader.getProperty(
              DatabricksJdbcUrlParams.USE_THRIFT_CLIENT.getParamName()));

      return DriverManager.getConnection(getFakeServiceJDBCUrl(), connectionProperties);
    }

    return DriverManager.getConnection(getJDBCUrl(), connectionProperties);
  }

  public static Connection getDogfoodJDBCConnection() throws SQLException {
    return DriverManager.getConnection(
        getDogfoodJDBCUrl(), getDatabricksUser(), getDatabricksDogfoodToken());
  }

  /** Used by the DBFSVolumeClient to bypass creation of connection */
  public static IDatabricksConnectionContext getDogfoodJDBCConnectionContext() throws SQLException {
    return DatabricksConnectionContextFactory.create(
        getDogfoodJDBCUrl(), getDatabricksUser(), getDatabricksDogfoodToken());
  }

  public static IDatabricksConnectionContext getDogfoodJDBCConnectionContext(
      List<List<String>> extraArgs) throws SQLException {
    String jdbcUrl = getDogfoodJDBCUrl();
    for (List<String> args : extraArgs) {
      jdbcUrl += ";" + args.get(0) + "=" + args.get(1);
    }
    return DatabricksConnectionContextFactory.create(
        jdbcUrl, getDatabricksUser(), getDatabricksDogfoodToken());
  }

  public static Connection getDogfoodJDBCConnection(List<List<String>> extraArgs)
      throws SQLException {
    String jdbcUrl = getDogfoodJDBCUrl();
    for (List<String> args : extraArgs) {
      jdbcUrl += ";" + args.get(0) + "=" + args.get(1);
    }
    return DriverManager.getConnection(jdbcUrl, getDatabricksUser(), getDatabricksDogfoodToken());
  }

  public static Connection getValidJDBCConnection(Map<String, String> args) throws SQLException {
    return DriverManager.getConnection(getJDBCUrl(args), getDatabricksUser(), getDatabricksToken());
  }

  public static Connection getBenchfoodJDBCConnection() throws SQLException {
    return DriverManager.getConnection(
        getBenchfoodJDBCUrl(), getDatabricksUser(), getDatabricksBenchfoodToken());
  }

  public static String getJDBCUrl() {
    String template = "jdbc:databricks://%s/default;ssl=1;AuthMech=3;httpPath=%s";
    String host = getDatabricksHost();
    String httpPath = getDatabricksHTTPPath();

    return String.format(template, host, httpPath);
  }

  public static String getJDBCUrl(Map<String, String> args) {
    String template = "jdbc:databricks://%s/default;ssl=1;AuthMech=3;httpPath=%s";

    String host = getDatabricksHost();
    String httpPath = getDatabricksHTTPPath();

    StringBuilder url = new StringBuilder(String.format(template, host, httpPath));
    for (Map.Entry<String, String> entry : args.entrySet()) {
      url.append(";");
      url.append(entry.getKey());
      url.append("=");
      url.append(entry.getValue());
    }

    return url.toString();
  }

  public static String getBenchfoodJDBCUrl() {
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=1;AuthMech=3;httpPath=%s";
    String host = getDatabricksBenchfoodHost();
    String httpPath = getDatabricksBenchfoodHTTPPath();

    return String.format(template, host, httpPath);
  }

  public static String getDogfoodJDBCUrl() {
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=1;AuthMech=3;httpPath=%s";
    String host = getDatabricksDogfoodHost();
    String httpPath = getDatabricksDogfoodHTTPPath();

    return String.format(template, host, httpPath);
  }

  public static boolean executeSQL(Connection conn, String sql) {
    try {
      conn.createStatement().execute(sql);
      return true;
    } catch (SQLException e) {
      System.out.println("Error executing SQL: " + e.getMessage());
      return false;
    }
  }

  public static ResultSet executeQuery(Connection conn, String sql) {
    try {
      ResultSet rs = conn.createStatement().executeQuery(sql);
      return rs;
    } catch (SQLException e) {
      System.out.println("Error executing SQL: " + e.getMessage());
      return null;
    }
  }

  public static void setupDatabaseTable(Connection conn, String tableName) {
    String tableDeletionSQL = "DROP TABLE IF EXISTS " + getFullyQualifiedTableName(tableName);
    executeSQL(conn, tableDeletionSQL);

    String tableCreationSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT PRIMARY KEY, col1 VARCHAR(255), col2 VARCHAR(255))";

    executeSQL(conn, tableCreationSQL);
  }

  public static void setupDatabaseTable(
      Connection conn, String tableName, String tableCreationSQL) {
    String tableDeletionSQL = "DROP TABLE IF EXISTS " + getFullyQualifiedTableName(tableName);

    executeSQL(conn, tableDeletionSQL);
    executeSQL(conn, tableCreationSQL);
  }

  public static void deleteTable(Connection conn, String tableName) {
    String SQL = "DROP TABLE IF EXISTS " + getFullyQualifiedTableName(tableName);
    executeSQL(conn, SQL);
  }

  public static String getFullyQualifiedTableName(String tableName) {
    return getDatabricksCatalog() + "." + getDatabricksSchema() + "." + tableName;
  }

  public static void insertTestDataForJoins(Connection conn, String table1Name, String table2Name) {
    // Insert data into the first table
    String insertTable1SQL1 =
        "INSERT INTO "
            + getFullyQualifiedTableName(table1Name)
            + " (id, col1, col2) VALUES (1, 'value1_table1', 'value2_table1')";
    executeSQL(conn, insertTable1SQL1);

    String insertTable1SQL2 =
        "INSERT INTO "
            + getFullyQualifiedTableName(table1Name)
            + " (id, col1, col2) VALUES (2, 'value3_table1', 'value4_table1')";
    executeSQL(conn, insertTable1SQL2);

    // Insert related data into the second table
    String insertTable2SQL1 =
        "INSERT INTO "
            + getFullyQualifiedTableName(table2Name)
            + " (id, col1, col2) VALUES (1, 'related_value1_table2', 'related_value2_table2')";
    executeSQL(conn, insertTable2SQL1);

    String insertTable2SQL2 =
        "INSERT INTO "
            + getFullyQualifiedTableName(table2Name)
            + " (id, col1, col2) VALUES (2, 'related_value3_table2', 'related_value4_table2')";
    executeSQL(conn, insertTable2SQL2);
  }

  public static void insertTestData(Connection conn, String tableName) {
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
    executeSQL(conn, insertSQL);
  }
}
