package com.databricks.client.jdbc;

import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.HTTP_PATH;
import static com.databricks.jdbc.integration.IntegrationTestUtil.getFullyQualifiedTableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.impl.DatabricksResultSetMetaData;
import com.databricks.jdbc.api.impl.arrow.ArrowResultChunk;
import com.databricks.jdbc.api.impl.volume.DatabricksVolumeClientFactory;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.sdk.service.sql.StatementState;
import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.*;
import java.util.StringJoiner;
import org.apache.http.entity.InputStreamEntity;
import org.junit.jupiter.api.Test;

/**
 * Example implementation demonstrating how to use the Databricks JDBC Driver in various scenarios.
 * Each method illustrates a different operation such as: - Connecting to a Databricks cluster or
 * SQL warehouse - Handling complex data types - Retrieving JDBC metadata - Executing statements
 * asynchronously - Using volumes (UC or DBFS) - Working with prepared/batch statements - Performing
 * OAuth/JWT-based authentication
 *
 * <p>If you wish to run these examples, please use your own Databricks workspace and token, you can
 * replace the httpPath and token in the example JDBC connections accordingly.
 */
public class DatabricksDriverExamples {

  private static final String DATABRICKS_TOKEN = System.getenv("DATABRICKS_EXAMPLE_TOKEN");

  /**
   * Utility method to print the contents of a {@link ResultSet}. It displays column names, data
   * types, precision, and row data.
   */
  public void printResultSet(ResultSet resultSet) throws SQLException {
    System.out.println("\n\nPrinting ResultSet contents:\n");
    ResultSetMetaData rsmd = resultSet.getMetaData();
    int columnsNumber = rsmd.getColumnCount();

    // Print column names
    for (int i = 1; i <= columnsNumber; i++) {
      System.out.print(rsmd.getColumnName(i) + "\t");
    }
    System.out.println();

    // Print column type names (e.g., INT, STRING, etc.)
    for (int i = 1; i <= columnsNumber; i++) {
      System.out.print(rsmd.getColumnTypeName(i) + "\t\t");
    }
    System.out.println();

    // Print column type codes (e.g., java.sql.Types)
    for (int i = 1; i <= columnsNumber; i++) {
      System.out.print(rsmd.getColumnType(i) + "\t\t\t");
    }
    System.out.println();

    // Print column precision
    for (int i = 1; i <= columnsNumber; i++) {
      System.out.print(rsmd.getPrecision(i) + "\t\t\t");
    }
    System.out.println();

    // Print column precision
    for (int i = 1; i <= columnsNumber; i++) {
      System.out.print(rsmd.isNullable(i) + "\t\t\t");
    }
    System.out.println();

    // Print row data
    while (resultSet.next()) {
      for (int i = 1; i <= columnsNumber; i++) {
        try {
          Object columnValue = resultSet.getObject(i);
          System.out.print(columnValue + "\t\t");
        } catch (Exception e) {
          // Certain columns might be absent or throw exceptions in edge cases
          System.out.print("NULL\t\t");
        }
      }
      System.out.println();
    }
  }

  /**
   * Demonstrates how SQLState is set/returned when an error occurs, for example using an invalid
   * SQL query.
   */
  @Test
  void exampleSeaSqlState() throws Exception {
    // Register the Databricks JDBC driver
    DriverManager.registerDriver(new Driver());

    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;"
            + "EnableTelemetry=1";

    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");
    Statement s = con.createStatement();

    // Attempt a bad query to illustrate how we catch and analyze DatabricksSQLException
    try {
      s.executeQuery("some fake sql query");
    } catch (DatabricksSQLException e) {
      System.out.println("Error message: " + e.getMessage());
      if (e.getSQLState() != null && !Objects.equals(e.getSQLState(), "")) {
        System.out.println("SQL State: " + e.getSQLState());
      }
    }
    con.close();
  }

  /**
   * Demonstrates how to retrieve a list of tables from Databricks via JDBC metadata, verifying
   * statement execution and printing the result.
   */
  @Test
  void exampleGetTables() throws Exception {
    DriverManager.registerDriver(new Driver());

    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;"
            + "EnableTelemetry=1";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    // Retrieve tables via DatabaseMetaData
    ResultSet rs = con.getMetaData().getTables("main", "jdbc_test_schema", "%", null);
    printResultSet(rs);
    rs.close();
    con.close();
  }

  /**
   * Demonstrates how to handle complex data types (Array, Map, Struct) when Arrow client is used
   * (default arrow-based fetch).
   */
  @Test
  void exampleComplexDataTypes() throws Exception {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;EnableComplexDatatypeSupport=1";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    // Example of retrieving an Array column
    System.out.println("\n-- Example Array --");
    ResultSet rs1 = con.createStatement().executeQuery("SELECT array(1, 4, 2, 5, 3, 6)");
    printResultSet(rs1);
    rs1.close();

    // Example of retrieving a Map column
    System.out.println("\n-- Example Map --");
    ResultSet rs2 =
        con.createStatement().executeQuery("SELECT map(1, 'one', 2, 'two', 3, 'three')");
    printResultSet(rs2);
    rs2.close();

    // Example of retrieving a Struct column
    System.out.println("\n-- Example Struct --");
    ResultSet rs3 =
        con.createStatement().executeQuery("SELECT named_struct('key1', 1, 'key2', 'value2')");
    printResultSet(rs3);
    rs3.close();

    con.close();
  }

  /**
   * Demonstrates how to handle complex data types (Array, Map, Struct) when Thrift client is used
   * (useThriftClient=1).
   */
  @Test
  void exampleComplexDataTypesThrift() throws Exception {
    DriverManager.registerDriver(new Driver());
    // You can replace the httpPath or token as needed
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "transportMode=https;ssl=1;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;usethriftclient=1;EnableComplexDatatypeSupport=1";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    // Array example
    System.out.println("\n-- Thrift Example Array --");
    ResultSet rs1 = con.createStatement().executeQuery("SELECT array(1, 4, 2, 5, 3, 6)");
    printResultSet(rs1);
    rs1.close();

    // Map example
    System.out.println("\n-- Thrift Example Map --");
    ResultSet rs2 =
        con.createStatement().executeQuery("SELECT map(1, 'one', 2, 'two', 3, 'three')");
    printResultSet(rs2);
    rs2.close();

    // Struct example
    System.out.println("\n-- Thrift Example Struct --");
    ResultSet rs3 =
        con.createStatement().executeQuery("SELECT named_struct('key1', 1, 'key2', 'value2')");
    printResultSet(rs3);
    rs3.close();

    con.close();
  }

  /**
   * Demonstrates retrieving ResultSet metadata (column precision, type, etc.) from a Databricks
   * table.
   */
  @Test
  void exampleResultSetMetaData() throws Exception {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/58aa1b363649e722";

    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established with jdbc driver......");

    Statement statement = con.createStatement();
    statement.setMaxRows(10000);

    // Example query to retrieve a subset of data
    ResultSet rs =
        statement.executeQuery(
            "select * from ml.feature_store_ol_dynamodb_.test_ft_data_types LIMIT 10");
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
  }

  /** Demonstrates GCP Service Account OAuth M2M connection. You can replace the token if needed. */
  @Test
  void exampleGcpServiceAccountOauthM2M() throws Exception {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl =
        "jdbc:databricks://3396486410346666.6.gcp.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=11;AuthFlow=1;OAuth2ClientId=your_client_id;OAuth2Secret=your_client_secret;httpPath=/sql/1.0/warehouses/2ce45060dd7d2a66;"
            + "GoogleServiceAccount=x-compute@developer.gserviceaccount.com";

    Connection con = DriverManager.getConnection(jdbcUrl);
    System.out.println("Connection established with jdbc driver......");
    Statement statement = con.createStatement();
    statement.setMaxRows(10000);

    // Simple query
    ResultSet rs = statement.executeQuery("select 1");
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
  }

  /**
   * Demonstrates GCP Credential JSON OAuth M2M connection. If you have a JSON credentials file,
   * specify it in 'GoogleCredentialsFile'.
   */
  @Test
  void exampleGcpCredentialJsonOauthM2M() throws Exception {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl =
        "jdbc:databricks://3396486410346666.6.gcp.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/2ce45060dd7d2a66;"
            + "GoogleCredentialsFile=<path_to_json_credential_file>";

    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established with jdbc driver......");
    Statement statement = con.createStatement();
    statement.setMaxRows(10000);

    ResultSet rs = statement.executeQuery("select 1");
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
  }

  /** Demonstrates how to query driver property info from a given JDBC URL. */
  @Test
  void exampleGetPropertyInfo() throws Exception {
    DriverManager.registerDriver(new Driver());
    String emptyJdbcUrl = "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com";

    // Retrieve driver property info for an "empty" URL
    DriverPropertyInfo[] driverPropertyInfos =
        new Driver().getPropertyInfo(emptyJdbcUrl, new Properties());
    assertEquals(1, driverPropertyInfos.length);
    assertEquals(HTTP_PATH.getParamName(), driverPropertyInfos[0].name);

    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com;AuthMech=11;Auth_Flow=0;"
            + "httpPath=/sql/1.0/warehouses/58aa1b363649e722;loglevel=1";
    driverPropertyInfos = new Driver().getPropertyInfo(jdbcUrl, new Properties());
    for (DriverPropertyInfo driverPropertyInfo : driverPropertyInfos) {
      if (driverPropertyInfo.required) {
        System.out.println(driverPropertyInfo.name + " " + driverPropertyInfo.description);
      }
    }
  }

  /**
   * Demonstrates how to query Databricks result set "disposition" and see if Cloud Fetch is in use
   * (via DatabricksResultSetMetaData).
   */
  @Test
  void exampleGetDisposition() throws Exception {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "transportMode=https;ssl=1;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established. Arrow is enabled by default......");

    String query = "SELECT * FROM RANGE(10)";
    ResultSet resultSet = con.createStatement().executeQuery(query);

    // Cast metadata to Databricks-specific class
    DatabricksResultSetMetaData rsmd = (DatabricksResultSetMetaData) resultSet.getMetaData();
    System.out.println("isCloudFetchUsed when arrow is enabled: " + rsmd.getIsCloudFetchUsed());

    resultSet.close();
    con.close();
  }

  /**
   * Demonstrates a simple query on an Arclight staging environment to illustrate general
   * connectivity.
   */
  @Test
  void exampleArclightConnection() throws Exception {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl =
        "jdbc:databricks://arclight-staging-e2-arclight-dmk-qa-staging-us-east-1.staging.cloud.databricks.com:443/"
            + "default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/8561171c1d9afb1f;";

    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    Statement statement = con.createStatement();
    statement.setMaxRows(10000);

    // Query some sample data
    ResultSet rs =
        statement.executeQuery(
            "select * from `arclight-dmk-catalog`.default.test_large_table limit 10");
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
    System.out.println("Query & print finished successfully.");
  }

  /** Demonstrates checking SQLState for a Thrift-based connection when a bad query is executed. */
  @Test
  void exampleThriftSqlState() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "ssl=1;AuthMech=3;"
            + "httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    Statement s = con.createStatement();
    try {
      s.executeQuery("some fake sql");
    } catch (DatabricksSQLException e) {
      System.out.println("Error message: " + e.getMessage());
      if (e.getSQLState() != null && !Objects.equals(e.getSQLState(), "")) {
        System.out.println("SQL State: " + e.getSQLState());
      }
    }
    con.close();
  }

  /** Demonstrates connecting to an all-purpose cluster on Databricks and running a simple query. */
  @Test
  void exampleAllPurposeClusters() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "ssl=1;AuthMech=3;"
            + "httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    Statement s = con.createStatement();
    s.executeQuery("SELECT * from RANGE(5)");
    con.close();
    System.out.println("Connection closed successfully......");
  }

  /**
   * Demonstrates inline queries that retrieve special data types (e.g., bytes, structs) from an
   * all-purpose cluster with Arrow disabled (enableArrow=0).
   */
  @Test
  void exampleAllPurposeClustersInline() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "ssl=1;AuthMech=3;"
            + "httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;enableArrow=0";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    Statement s = con.createStatement();

    // Example retrieving bytes
    ResultSet rs = s.executeQuery("SELECT unhex('f000')");
    rs.next();
    System.out.println("Bytes from unhex('f000'): " + Arrays.toString(rs.getBytes(1)));

    // Example retrieving a struct
    rs = s.executeQuery("SELECT struct(1 as a, 2 as b)");
    rs.next();
    System.out.println("Struct object: " + rs.getObject(1));

    con.close();
    System.out.println("Connection closed successfully......");
  }

  /**
   * Demonstrates how to query schema/catalog/other metadata from an all-purpose cluster, such as
   * getting the list of functions matching a certain pattern.
   */
  @Test
  void exampleAllPurposeClustersMetadata() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "AuthMech=3;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    // Example: retrieve functions with name matching "current_%"
    ResultSet resultSet =
        con.getMetaData()
            .getFunctions("uc_1716360380283_cata", "uc_1716360380283_db1", "current_%");
    printResultSet(resultSet);
    resultSet.close();
    con.close();
  }

  /** Demonstrates how to enable logging at a particular level, specify log paths, etc. */
  @Test
  void exampleLogging() throws Exception {
    DriverManager.registerDriver(new Driver());

    // Logging parameters specified in the JDBC URL
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "transportMode=https;ssl=1;"
            + "httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;"
            + "AuthMech=3;UID=token;LogLevel=debug;LogPath=./logDir;LogFileCount=3;LogFileSize=2;";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    // Sample query
    ResultSet resultSet =
        con.createStatement()
            .executeQuery("SELECT * from lb_demo.demographics_fs.demographics LIMIT 10");
    printResultSet(resultSet);
    resultSet.close();
    con.close();
  }

  /**
   * Demonstrates conversion of date/bigint/decimal columns into Java's LocalDate, BigInteger,
   * BigDecimal by calling getObject with the appropriate class type.
   */
  @Test
  void exampleDatatypeConversion() throws SQLException {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "transportMode=https;ssl=1;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    // Example table containing columns of date, bigint, decimal, etc.
    String selectSQL =
        "SELECT id, local_date, big_integer, big_decimal FROM main.jdbc_test_schema.test_table_typeconversion";
    ResultSet rs = con.createStatement().executeQuery(selectSQL);
    printResultSet(rs);

    // Retrieving columns with typed getObject(...) calls
    LocalDate date = rs.getObject("local_date", LocalDate.class);
    System.out.println("LocalDate: " + date + " (class: " + date.getClass() + ")");

    BigInteger bigInteger = rs.getObject("big_integer", BigInteger.class);
    System.out.println("BigInteger: " + bigInteger + " (class: " + bigInteger.getClass() + ")");

    BigDecimal bigDecimal = rs.getObject("big_decimal", BigDecimal.class);
    System.out.println("BigDecimal: " + bigDecimal + " (class: " + bigDecimal.getClass() + ")");

    con.close();
  }

  /** Demonstrates configuration of HTTP retry flags, such as TemporarilyUnavailableRetry. */
  @Test
  void exampleHttpFlags() throws Exception {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "transportMode=https;ssl=1;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;TemporarilyUnavailableRetry=3;";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");
    con.close();
  }

  /**
   * Demonstrates Unity Catalog (UC) volume operations (PUT, GET, DELETE) using InputStream. Assumes
   * the user has privileges to ingest to UC volumes, and the volume path is valid.
   */
  @Test
  void exampleUCVolumeUsingInputStream() throws Exception {
    DriverManager.registerDriver(new Driver());
    System.out.println("Starting UC volume test...");

    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "transportMode=http;ssl=1;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";

    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);

    // Example setting an allowed ingestion path
    con.setClientInfo(DatabricksJdbcConstants.ALLOWED_VOLUME_INGESTION_PATHS, "delete");
    System.out.println("Connection created.");

    var client = DatabricksVolumeClientFactory.getVolumeClient(con);

    File file = new File("/tmp/put.txt");
    try {
      Files.writeString(file.toPath(), "test-put");
      System.out.println("File created at /tmp/put.txt");

      // Put object
      System.out.println(
          "Object inserted: "
              + client.putObject(
                  "main",
                  "jdbc_test_schema",
                  "jdbc_test_volume",
                  "test-stream.csv",
                  new FileInputStream(file),
                  file.length(),
                  true));

      // Get object
      InputStreamEntity inputStream =
          client.getObject("main", "jdbc_test_schema", "jdbc_test_volume", "test-stream.csv");
      System.out.println("Got data: " + new String(inputStream.getContent().readAllBytes()));
      inputStream.getContent().close();

      // Check existence
      System.out.println(
          "Object exists? "
              + client.objectExists(
                  "main", "jdbc_test_schema", "jdbc_test_volume", "test-stream.csv", false));

      // Delete object
      client.deleteObject("main", "jdbc_test_schema", "jdbc_test_volume", "test-stream.csv");
      System.out.println(
          "Object exists after deletion? "
              + client.objectExists(
                  "main", "jdbc_test_schema", "jdbc_test_volume", "test-stream.csv", false));
    } finally {
      file.delete();
      con.close();
    }
  }

  /**
   * Demonstrates DBFS volume operations (PUT, GET, LIST, DELETE) using streams. Replace the
   * relevant path or volume name if you use a different setup.
   */
  @Test
  void exampleDBFSVolumeOperationUsingStream() throws Exception {
    System.out.println("Starting DBFS volume test...");

    // You can replace the token if using a different workspace/token
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "transportMode=http;ssl=1;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/58aa1b363649e722;Loglevel=debug;";

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(jdbcUrl, "token", DATABRICKS_TOKEN);
    var client = DatabricksVolumeClientFactory.getVolumeClient(connectionContext);

    File file = new File("/tmp/put.txt");
    try {
      Files.writeString(file.toPath(), "test-put");
      System.out.println("File created at /tmp/put.txt");

      System.out.println(
          "Object inserted: "
              + client.putObject(
                  "main",
                  "jdbc_test_schema",
                  "jdbc_test_volume",
                  "test-stream.csv",
                  new FileInputStream(file),
                  file.length(),
                  true));

      InputStreamEntity inputStream =
          client.getObject("main", "jdbc_test_schema", "jdbc_test_volume", "test-stream.csv");
      System.out.println("Got data: " + new String(inputStream.getContent().readAllBytes()));
      inputStream.getContent().close();

      System.out.println(
          "Listed objects: "
              + client.listObjects("main", "jdbc_test_schema", "jdbc_test_volume", "test", false));

      System.out.println(
          "Delete object result: "
              + client.deleteObject(
                  "main", "jdbc_test_schema", "jdbc_test_volume", "test-stream.csv"));
    } finally {
      file.delete();
    }
  }

  /**
   * Demonstrates DBFS volume operations with local file paths instead of streams. (PUT, GET, LIST,
   * DELETE)
   */
  @Test
  void exampleDBFSVolumeOperation() throws Exception {
    System.out.println("Starting DBFS volume test...");

    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "transportMode=http;ssl=1;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/58aa1b363649e722;Loglevel=debug;VolumeOperationAllowedLocalPaths=/tmp;";

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(jdbcUrl, "token", DATABRICKS_TOKEN);
    var client = DatabricksVolumeClientFactory.getVolumeClient(connectionContext);

    File file = new File("/tmp/put.txt");
    File fileGet = new File("/tmp/dbfs.txt");

    try {
      Files.writeString(file.toPath(), "test-put");
      System.out.println("File created at /tmp/put.txt");

      System.out.println(
          "Object inserted: "
              + client.putObject(
                  "main",
                  "jdbc_test_schema",
                  "jdbc_test_volume",
                  "test-stream.csv",
                  "/tmp/put.txt",
                  true));

      System.out.println(
          "Get object result: "
              + client.getObject(
                  "main",
                  "jdbc_test_schema",
                  "jdbc_test_volume",
                  "test-stream.csv",
                  "/tmp/dbfs.txt"));

      System.out.println(
          "List objects: "
              + client.listObjects("main", "jdbc_test_schema", "jdbc_test_volume", "test", false));

      System.out.println(
          "Delete object result: "
              + client.deleteObject(
                  "main", "jdbc_test_schema", "jdbc_test_volume", "test-stream.csv"));
    } finally {
      file.delete();
      fileGet.delete();
    }
  }

  /**
   * Demonstrates using prepared statements with a large number of parameters by setting
   * 'supportManyParameters=1' in the connection URL.
   */
  @Test
  public void exampleTooManyParameters() throws SQLException {
    DriverManager.registerDriver(new Driver());

    // Building a query with 300 parameters in the IN clause
    StringBuilder sql =
        new StringBuilder("SELECT * FROM lb_demo.demographics_fs.demographics WHERE age IN (");
    StringJoiner joiner = new StringJoiner(",");
    for (int i = 0; i < 300; i++) {
      joiner.add("?");
    }
    sql.append(joiner).append(")");
    System.out.println("SQL: " + sql);

    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "transportMode=https;ssl=1;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;supportManyParameters=1";

    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    PreparedStatement pstmt = con.prepareStatement(sql.toString());

    // Fill parameters
    List<Integer> ids = new ArrayList<>();
    for (int i = 1; i <= 300; i++) {
      ids.add(i);
    }
    for (int i = 0; i < ids.size(); i++) {
      pstmt.setInt(i + 1, ids.get(i));
    }

    ResultSet rs = pstmt.executeQuery();
    printResultSet(rs);
    rs.close();
    con.close();
  }

  /**
   * Demonstrates error handling on an all-purpose cluster with direct results enabled
   * (enableDirectResults=1).
   */
  @Test
  void exampleAllPurposeClusters_errorHandling() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "ssl=1;AuthMech=3;"
            + "httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;"
            + "enableDirectResults=1";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    Statement s = con.createStatement();
    s.executeQuery("SELECT * from RANGE(10)");
    con.close();
    System.out.println("Connection closed successfully......");
  }

  /**
   * Demonstrates asynchronous statement execution on an all-purpose cluster, polling the statement
   * status until it succeeds or fails.
   */
  @Test
  void exampleAllPurposeClusters_async() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "ssl=1;AuthMech=3;"
            + "httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;"
            + "enableDirectResults=1";

    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established... (con1)");

    Statement s = con.createStatement();
    IDatabricksStatement ids = s.unwrap(IDatabricksStatement.class);

    // Attempt drop if it exists
    try {
      s.execute("DROP TABLE JDBC_ASYNC_CLUSTER");
    } catch (Exception ignore) {
    }

    long initialTime = System.currentTimeMillis();
    String sql =
        "CREATE TABLE JDBC_ASYNC_CLUSTER AS ("
            + "  SELECT * FROM ("
            + "    SELECT * FROM ("
            + "      SELECT t1.*"
            + "      FROM main.streaming.random_large_table t1"
            + "      INNER JOIN main.streaming.random_large_table t2"
            + "      ON t1.prompt = t2.prompt"
            + "    ) nested_t1"
            + "  ) nested_t2"
            + ")";

    // Execute asynchronously
    ResultSet rs = ids.executeAsync(sql);
    StatementState state = rs.unwrap(IDatabricksResultSet.class).getStatementStatus().getState();
    System.out.println("Initial state: " + state);
    System.out.println("Time taken: " + (System.currentTimeMillis() - initialTime));
    System.out.println("StatementId: " + rs.unwrap(IDatabricksResultSet.class).getStatementId());

    // Poll for status
    int count = 1;
    while (state != StatementState.SUCCEEDED && state != StatementState.FAILED) {
      Thread.sleep(1000);
      rs = s.unwrap(IDatabricksStatement.class).getExecutionResult();
      state = rs.unwrap(IDatabricksResultSet.class).getStatementStatus().getState();
      System.out.println(
          "Status: "
              + state
              + ", attempt "
              + count++
              + ", time taken "
              + (System.currentTimeMillis() - initialTime));
    }

    // Use a second connection to check status
    Connection con2 = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established... (con2)");
    IDatabricksConnection idc = con2.unwrap(IDatabricksConnection.class);
    Statement stm = idc.getStatement(rs.unwrap(IDatabricksResultSet.class).getStatementId());
    ResultSet rs2 = stm.unwrap(IDatabricksStatement.class).getExecutionResult();

    System.out.println(
        "Async execution final status (con2): "
            + rs2.unwrap(IDatabricksResultSet.class).getStatementStatus().getState());

    // Cleanup
    stm.cancel();
    stm.execute("DROP TABLE JDBC_ASYNC_CLUSTER");
    System.out.println("Statement cancelled & table dropped (con2).");

    s.close();
    con2.close();
    con.close();
    System.out.println("Connections closed successfully.");
  }

  /**
   * Demonstrates asynchronous execution on a DBSQL warehouse. Similar approach to the all-purpose
   * clusters example.
   */
  @Test
  void exampleDBSQL_async() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "ssl=1;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;enableDirectResults=1";

    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established... (con1)");

    Statement s = con.createStatement();
    IDatabricksStatement ids = s.unwrap(IDatabricksStatement.class);

    try {
      s.execute("DROP TABLE JDBC_ASYNC_DBSQL");
    } catch (Exception ignore) {
    }

    long initialTime = System.currentTimeMillis();
    String sql =
        "CREATE TABLE JDBC_ASYNC_DBSQL AS ("
            + "  SELECT * FROM ("
            + "    SELECT * FROM ("
            + "      SELECT t1.*"
            + "      FROM main.streaming.random_large_table t1"
            + "      INNER JOIN main.streaming.random_large_table t2"
            + "      ON t1.prompt = t2.prompt"
            + "    ) nested_t1"
            + "  ) nested_t2"
            + ")";

    // Execute asynchronously
    ResultSet rs = ids.executeAsync(sql);
    StatementState state = rs.unwrap(IDatabricksResultSet.class).getStatementStatus().getState();
    System.out.println("Time taken (initial): " + (System.currentTimeMillis() - initialTime));
    System.out.println("Initial state: " + state);

    // Poll for status
    int count = 1;
    while (state != StatementState.SUCCEEDED && state != StatementState.FAILED) {
      Thread.sleep(1000);
      rs = s.unwrap(IDatabricksStatement.class).getExecutionResult();
      state = rs.unwrap(IDatabricksResultSet.class).getStatementStatus().getState();
      System.out.println(
          "Status: "
              + state
              + ", attempt "
              + count++
              + ", time taken "
              + (System.currentTimeMillis() - initialTime));
    }

    // Second connection
    Connection con2 = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established... (con2)");
    IDatabricksConnection idc = con2.unwrap(IDatabricksConnection.class);
    Statement stm = idc.getStatement(rs.unwrap(IDatabricksResultSet.class).getStatementId());
    ResultSet rs2 = stm.unwrap(IDatabricksStatement.class).getExecutionResult();

    System.out.println(
        "Final status (con2): "
            + rs2.unwrap(IDatabricksResultSet.class).getStatementStatus().getState());

    // Cleanup
    stm.cancel();
    stm.execute("DROP TABLE JDBC_ASYNC_DBSQL");
    System.out.println("Statement cancelled & table dropped (con2).");

    s.close();
    con2.close();
    con.close();
    System.out.println("Connections closed successfully.");
  }

  /** Demonstrates how to close a connection by session ID on an all-purpose cluster. */
  @Test
  void exampleAllPurposeClusters_closeBySessionId() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "ssl=1;AuthMech=3;"
            + "httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;enableDirectResults=1";

    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established (con1)...");
    Statement s = con.createStatement();

    try {
      s.execute("DROP TABLE IF EXISTS JDBC_ASYNC_CLUSTER");
    } catch (Exception ignore) {
    }

    // Simple test query
    s.executeQuery("Select 1");

    String connectionId = con.unwrap(IDatabricksConnection.class).getConnectionId();

    // Properties for closing by session ID (PWD must be the token)
    Properties p = new Properties();
    p.setProperty("PWD", DATABRICKS_TOKEN);

    // Close connection by session ID
    Driver.getInstance().closeConnection(jdbcUrl, p, connectionId);

    // Now any statement on 'con' should fail
    assertThrows(DatabricksSQLException.class, () -> s.executeQuery("Select 1"));
  }

  /** Demonstrates how to close a connection by session ID on a DBSQL warehouse. */
  @Test
  void exampleDBSQL_closeBySessionId() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "ssl=1;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;enableDirectResults=1";

    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established (con1)...");
    Statement s = con.createStatement();

    try {
      s.execute("DROP TABLE JDBC_ASYNC_DBSQL");
    } catch (Exception ignore) {
    }

    // Simple test query
    s.executeQuery("Select 1");

    String connectionId = con.unwrap(IDatabricksConnection.class).getConnectionId();

    Properties p = new Properties();
    p.setProperty("PWD", DATABRICKS_TOKEN);

    // Close connection by session ID
    Driver.getInstance().closeConnection(jdbcUrl, p, connectionId);

    // Any statement on 'con' should now fail
    assertThrows(DatabricksSQLException.class, () -> s.executeQuery("Select 1"));
  }

  /**
   * Demonstrates batch statement execution (with addBatch/executeBatch) on an all-purpose cluster.
   * The 'MaxBatchSize=4' parameter in the JDBC URL indicates how many statements can be grouped at
   * once on the server side.
   */
  @Test
  void exampleBatchAllPurposeClusters() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "ssl=1;AuthMech=3;"
            + "httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;MaxBatchSize=4";
    String tableName = "batch_test_table";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    Statement s = con.createStatement();
    s.addBatch("DROP TABLE IF EXISTS " + getFullyQualifiedTableName(tableName));
    s.addBatch(
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT PRIMARY KEY, col1 VARCHAR(255), col2 VARCHAR(255))");
    s.executeBatch();
    s.clearBatch();

    s.addBatch(
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')");
    s.addBatch(
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (2, 'value3', 'value4')");
    s.addBatch(
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (3, 'value5', 'value6')");
    s.addBatch(
        "UPDATE "
            + getFullyQualifiedTableName(tableName)
            + " SET col1 = 'updatedValue1' WHERE id = 1");

    System.out.println("Batch result: " + Arrays.toString(s.executeBatch()));
    s.clearBatch();
    con.close();
    System.out.println("Connection closed successfully......");
  }

  /**
   * Demonstrates batch inserts using a PreparedStatement. The logic shows how to accumulate
   * multiple parameter sets in one batch.
   */
  @Test
  void exampleBatchFunction() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "transportMode=http;ssl=1;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/dd43ee29fedd958d;";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    String sqlStatement =
        "INSERT INTO main.jdbc_test_schema.diamonds (carat, cut, color, clarity) VALUES (?, ?, ?, ?)";
    PreparedStatement pstmt = con.prepareStatement(sqlStatement);

    // Add several parameter sets to the batch
    for (int i = 1; i <= 3; i++) {
      pstmt.setFloat(1, 0.23f);
      pstmt.setString(2, "OK");
      pstmt.setString(3, "E");
      pstmt.setString(4, "SI2");
      pstmt.addBatch();
    }

    // Example of a different data type that might fail if schema doesn't allow strings for 'carat'
    pstmt.setString(1, "malformed");
    pstmt.setString(2, "Bad");
    pstmt.setString(3, "F");
    pstmt.setString(4, "SI6");
    pstmt.addBatch();

    // Add more parameter sets
    for (int i = 1; i <= 3; i++) {
      pstmt.setFloat(1, 0.23f);
      pstmt.setString(2, "Bad");
      pstmt.setString(3, "F");
      pstmt.setString(4, "SI6");
      pstmt.addBatch();
    }

    // Execute the batch
    int[] updateCounts = pstmt.executeBatch();
    for (int count : updateCounts) {
      System.out.println("Update count: " + count);
    }
    con.close();
  }

  /**
   * Demonstrates JWT-based M2M flow using a private key and passphrase (for instance with Okta as
   * the OAuth provider). The parameters in the JDBC URL can be adapted for your OAuth2 provider, if
   * needed.
   */
  @Test
  void exampleM2MJWT() throws SQLException {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "httpPath=sql/1.0/warehouses/58aa1b363649e722;"
            + "AuthMech=11;ssl=1;Auth_Flow=1;"
            + "OAuth2TokenEndpoint=https://dev-591123.oktapreview.com/oauth2/aus1mzu4zk5TWwMvx0h8/v1/token;"
            + "Auth_Scope=sql;OAuth2ClientId=0oa25wnir4ehnKDj10h8;"
            + "Auth_KID=EbKQzTAVP1_3E59Bq5P3Uv8krHCpj3hIWTodcmDwQ5k;"
            + "UseJWTAssertion=1;"
            + "Auth_JWT_Key_File=jdbc-testing-enc.pem;"
            + "Auth_JWT_Key_Passphrase=s3cr3t";

    Connection con = DriverManager.getConnection(jdbcUrl);
    System.out.println("Connection established via JWT M2M OAuth...");

    ResultSet rs = con.createStatement().executeQuery("SELECT 1");
    printResultSet(rs);
    rs.close();
    con.close();
  }

  /**
   * Demonstrates chunk download retry logic by injecting artificial errors in ArrowResultChunk (for
   * advanced debugging).
   */
  @Test
  void exampleChunkDownloadRetry() throws Exception {
    // Enable error injection for demonstration
    ArrowResultChunk.enableErrorInjection();
    ArrowResultChunk.setErrorInjectionCountMaxValue(2);

    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;"
            + "ssl=1;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/58aa1b363649e722";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    Statement s = con.createStatement();
    // Large RANGE to force chunk downloads
    s.executeQuery("SELECT * from RANGE(37500000)");

    ResultSet rs = s.getResultSet();
    while (rs.next())
      ;

    con.close();
    System.out.println("Connection closed successfully......");

    // Disable error injection after the test
    ArrowResultChunk.disableErrorInjection();
  }
}
