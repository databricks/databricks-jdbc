package com.databricks.client.jdbc;

import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.HTTP_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.impl.DatabricksResultSetMetaData;
import com.databricks.jdbc.api.impl.volume.DatabricksVolumeClientFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksVolumeOperationException;
import com.databricks.jdbc.model.client.filesystem.VolumePutResult;
import com.databricks.sdk.service.sql.StatementState;
import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
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

  private static final String JDBC_URL_WAREHOUSE =
      "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;"
          + "transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/999999999999;";
  private static final String JDBC_URL_CLUSTER =
      "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;"
          + "transportMode=http;ssl=1;httpPath=sql/protocolv1/o/9999999999999999/9999999999999999;AuthMech=3;";
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

    // Print column nullable
    for (int i = 1; i <= columnsNumber; i++) {
      System.out.print(rsmd.isNullable(i) + "\t\t\t");
    }

    System.out.println();

    // Print column display size
    for (int i = 1; i <= columnsNumber; i++) {
      System.out.print(rsmd.getColumnDisplaySize(i) + "\t\t\t");
    }

    System.out.println();

    int rows = 0;
    // Print row data
    while (resultSet.next()) {
      rows++;
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
    System.out.println("Total rows: " + rows);
  }

  /** Demonstrates usage of DefaultStringColumnLength parameter */
  @Test
  void exampleDefaultStringColumnLength() throws Exception {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl = JDBC_URL_WAREHOUSE + "EnableTelemetry=1" + ";DefaultStringColumnLength=3";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("select 'string' as col");
    printResultSet(rs);
    stmt.close();
    con.close();
  }

  /*
   * Demonstrates use of url param RowsFetchedPerBlock. The maximum number of rows that a query returns at a time.
   * Works only with thrift inline mode
   */
  @Test
  void exampleRowsFetchedPerBlock() throws Exception {
    // Register the Databricks JDBC driver
    DriverManager.registerDriver(new Driver());
    String jdbcUrl =
        JDBC_URL_WAREHOUSE + "EnableTelemetry=1" + ";enableArrow=0" + ";RowsFetchedPerBlock=3";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM RANGE(12)"); // 4 FetchResults calls made
    printResultSet(rs);
    stmt.close();
    rs.close();
    con.close();
  }

  /** Demonstrates use statement.setMaxRows(). Limits the number of rows returned by a query. */
  @Test
  void exampleMaxRows() throws Exception {
    // Register the Databricks JDBC driver
    DriverManager.registerDriver(new Driver());
    String jdbcUrl = JDBC_URL_WAREHOUSE + "EnableTelemetry=1" + "enableArrow=0";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    Statement stmt = con.createStatement();
    stmt.setMaxRows(5);
    ResultSet rs = stmt.executeQuery("SELECT * FROM RANGE(10)");
    printResultSet(rs); // 5 rows will be printed
    stmt.close();
    rs.close();
    con.close();
  }

  /**
   * Demonstrates how SQLState is set/returned when an error occurs, for example using an invalid
   * SQL query.
   */
  @Test
  void exampleSeaSqlState() throws Exception {
    // Register the Databricks JDBC driver
    DriverManager.registerDriver(new Driver());

    String jdbcUrl = JDBC_URL_WAREHOUSE + "EnableTelemetry=1";

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

    String jdbcUrl = JDBC_URL_WAREHOUSE + "EnableTelemetry=1";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    // Retrieve tables via DatabaseMetaData
    ResultSet rs = con.getMetaData().getTables("main", "jdbc_test_schema", "%", null);
    printResultSet(rs);
    rs.close();
    con.close();
  }

  /**
   * Demonstrates how to retrieve a list of imported keys from Databricks via JDBC metadata,
   * verifying statement execution and printing the result.
   */
  @Test
  void exampleGetImportedKeys() throws Exception {
    DriverManager.registerDriver(new Driver());

    String jdbcUrl =
        JDBC_URL_WAREHOUSE + "EnableTelemetry=1;UseThriftClient=1;LogLevel=6;LogPath=/tmp";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    // Retrieve imported keys via DatabaseMetaData
    ResultSet rs = con.getMetaData().getImportedKeys("field_demos", "gopaldb", "Purchases");

    printResultSet(rs);
    rs.close();
    con.close();
  }

  /**
   * Demonstrates how to retrieve a list of imported keys from Databricks via JDBC metadata,
   * verifying statement execution and printing the result.
   */
  @Test
  void exampleGetCrossReferences() throws Exception {
    DriverManager.registerDriver(new Driver());

    String jdbcUrl = JDBC_URL_WAREHOUSE + "EnableTelemetry=1;UseThriftClient=1";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    // Retrieve imported keys via DatabaseMetaData
    ResultSet rs =
        con.getMetaData()
            .getCrossReference(
                "field_demos", "gopaldb", "orders_fk", "field_demos", "gopaldb", "Purchases");

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
    String jdbcUrl = JDBC_URL_WAREHOUSE + "EnableComplexDatatypeSupport=1";
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
    String jdbcUrl = JDBC_URL_WAREHOUSE + "usethriftclient=1;EnableComplexDatatypeSupport=1";
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
    String jdbcUrl = JDBC_URL_WAREHOUSE;

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
        "jdbc:databricks://3396486410346666.6.gcp.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=11;AuthFlow=1;OAuth2ClientId=your_client_id;OAuth2Secret=your_client_secret;httpPath=/sql/1.0/warehouses/999999999999;"
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
        "jdbc:databricks://3396486410346666.6.gcp.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/999999999999;"
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
    String emptyJdbcUrl = "jdbc:databricks://sample-host.cloud.databricks.com";

    // Retrieve driver property info for an "empty" URL
    DriverPropertyInfo[] driverPropertyInfos =
        new Driver().getPropertyInfo(emptyJdbcUrl, new Properties());
    assertEquals(1, driverPropertyInfos.length);
    assertEquals(HTTP_PATH.getParamName(), driverPropertyInfos[0].name);

    String jdbcUrl =
        "jdbc:databricks://sample-host.cloud.databricks.com;AuthMech=11;Auth_Flow=0;"
            + "httpPath=/sql/1.0/warehouses/999999999999;loglevel=1";
    driverPropertyInfos = new Driver().getPropertyInfo(jdbcUrl, new Properties());
    for (DriverPropertyInfo driverPropertyInfo : driverPropertyInfos) {
      if (driverPropertyInfo.required) {
        System.out.println(driverPropertyInfo.name + " " + driverPropertyInfo.description);
      }
    }
  }

  /**
   * Demonstrates how to query and see if Cloud Fetch is in use (via DatabricksResultSetMetaData).
   */
  @Test
  void exampleGetCloudFetchUsed() throws Exception {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl = JDBC_URL_WAREHOUSE + "UseThriftClient=0;EnableSqlExecHybridResults=1";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established. Arrow is enabled by default......");

    String query = "SELECT * FROM RANGE(1)";
    ResultSet smallResultSet = con.createStatement().executeQuery(query);

    // Cast metadata to Databricks-specific class
    DatabricksResultSetMetaData rsmd = (DatabricksResultSetMetaData) smallResultSet.getMetaData();
    System.out.println("isCloudFetchUsed for small query: " + rsmd.getIsCloudFetchUsed());
    smallResultSet.close();

    query = "SELECT * FROM RANGE(10000000)";
    ResultSet largeResultSet = con.createStatement().executeQuery(query);

    // Cast metadata to Databricks-specific class
    rsmd = (DatabricksResultSetMetaData) largeResultSet.getMetaData();
    System.out.println("isCloudFetchUsed for large query: " + rsmd.getIsCloudFetchUsed());
    largeResultSet.close();

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
        "jdbc:databricks://sample-host.cloud.databricks.com:443/"
            + "default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/999999999999;";

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
    String jdbcUrl = JDBC_URL_CLUSTER;
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
    String jdbcUrl = JDBC_URL_CLUSTER;
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
    String jdbcUrl = JDBC_URL_CLUSTER + "enableArrow=0";
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
    String jdbcUrl = JDBC_URL_CLUSTER;
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
        JDBC_URL_CLUSTER
            + "UID=token;LogLevel=debug;LogPath=./logDir;LogFileCount=3;LogFileSize=2;";
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
    String jdbcUrl = JDBC_URL_WAREHOUSE;
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
    String jdbcUrl = JDBC_URL_WAREHOUSE + "TemporarilyUnavailableRetry=3;";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");
    con.close();
  }

  @Test
  void examplePutFiles() throws Exception {

    Properties p = new Properties();
    p.setProperty(DatabricksJdbcConstants.ENABLE_VOLUME_OPERATIONS, "1");
    p.setProperty("PWD", DATABRICKS_TOKEN);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL_WAREHOUSE, p);
    com.databricks.jdbc.api.IDatabricksVolumeClient client =
        DatabricksVolumeClientFactory.getVolumeClient(connectionContext);
    int numFiles = 2;
    List<String> objectPaths = new ArrayList<>();
    List<String> localPaths = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      String objectPath = "test-stream-" + i + ".csv";
      String localPath = "/tmp/put-" + i + ".txt";
      objectPaths.add(objectPath);
      localPaths.add(localPath);
      File file = new File(localPath);
      java.nio.file.Files.write(file.toPath(), "test-put".getBytes());
      System.out.println("File created at " + localPath);
    }
    List<VolumePutResult> results =
        client.putFiles(
            "main", "jdbc_test_schema", "jdbc_test_volume", objectPaths, localPaths, true);

    for (int i = 0; i < numFiles; i++) {
      System.out.println(
          "status: "
              + results.get(i).getStatus()
              + ", error message: "
              + results.get(i).getMessage());
    }

    for (int i = 0; i < numFiles; i++) {
      File file = new File(localPaths.get(i));
      file.delete();
    }

    for (int i = 0; i < numFiles; i++) {
      System.out.println(
          "Delete object result: "
              + client.deleteObject(
                  "main", "jdbc_test_schema", "jdbc_test_volume", objectPaths.get(i)));
    }

    System.out.println("Files deleted.");
  }

  /**
   * Demonstrates DBFS volume operations with local file paths instead of streams. (PUT, GET, LIST,
   * DELETE) Enable enableVolumeOperations client property to perform these operations
   */
  @Test
  void exampleDBFSVolumeOperation() throws Exception {
    System.out.println("Starting DBFS volume test...");

    String jdbcUrl = JDBC_URL_WAREHOUSE + "Loglevel=debug;VolumeOperationAllowedLocalPaths=/tmp;";
    Properties p = new Properties();
    p.setProperty(DatabricksJdbcConstants.ENABLE_VOLUME_OPERATIONS, "1");
    p.setProperty("PWD", DATABRICKS_TOKEN);
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(jdbcUrl, p);
    com.databricks.jdbc.api.IDatabricksVolumeClient client =
        DatabricksVolumeClientFactory.getVolumeClient(connectionContext);

    File file = new File("/tmp/put.txt");
    File fileGet = new File("/tmp/dbfs.txt");

    try {
      java.nio.file.Files.write(file.toPath(), "test-put".getBytes());
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
   * Demonstrates DBFS volume operations(GET/PUT/DELETE) with stream failing without
   * enableVolumeOperations in client properties Setting this property from jdbcrUrl will not work.
   */
  @Test
  void exampleDBFSVolumeOperationWithoutVolumeOperationsEnabled() throws Exception {
    System.out.println("Starting DBFS volume test...");

    // You can replace the token if using a different workspace/token
    String jdbcUrl = JDBC_URL_WAREHOUSE + "Loglevel=debug;enableVolumeOperations=1";
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(jdbcUrl, "TOKEN", DATABRICKS_TOKEN);
    com.databricks.jdbc.api.IDatabricksVolumeClient client =
        DatabricksVolumeClientFactory.getVolumeClient(connectionContext);

    File file = new File("/tmp/put.txt");
    try {
      java.nio.file.Files.write(file.toPath(), "test-put".getBytes());
      System.out.println("File created at /tmp/put.txt");

      Exception putEx =
          assertThrows(
              DatabricksVolumeOperationException.class,
              () ->
                  client.putObject(
                      "main",
                      "jdbc_test_schema",
                      "jdbc_test_volume",
                      "test-stream.csv",
                      new FileInputStream(file),
                      file.length(),
                      true),
              "putObject should fail if enableVolumeOperations not present in client properties");
      System.out.println("putObject failed as expected: " + putEx.getMessage());

      Exception getEx =
          assertThrows(
              DatabricksVolumeOperationException.class,
              () -> {
                client.getObject("main", "jdbc_test_schema", "jdbc_test_volume", "test-stream.csv");
              },
              "getObject should fail if enableVolumeOperations not present in client properties");
      System.out.println("getObject failed as expected: " + getEx.getMessage());

      Exception delEx =
          assertThrows(
              DatabricksVolumeOperationException.class,
              () ->
                  client.deleteObject(
                      "main", "jdbc_test_schema", "jdbc_test_volume", "test-stream.csv"),
              "deleteObject should fail if enableVolumeOperations not present in client properties");
      System.out.println("deleteObject failed as expected: " + delEx.getMessage());

    } finally {
      file.delete();
    }
  }

  /** Demonstrates using prepared statements on DBSQL in Thrift mode (useThriftClient=1). */
  @Test
  public void exampleThriftPreparedStatements() throws SQLException {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl = JDBC_URL_WAREHOUSE + "usethriftclient=1";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", DATABRICKS_TOKEN);
    System.out.println("Connection established......");

    // Example query with a parameter
    String sql = "SELECT * FROM RANGE(?)";
    PreparedStatement pstmt = con.prepareStatement(sql);
    pstmt.setInt(1, 10);

    ResultSet rs = pstmt.executeQuery();
    printResultSet(rs);
    rs.close();
    con.close();
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

    String jdbcUrl = JDBC_URL_WAREHOUSE + "supportManyParameters=1";

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
    String jdbcUrl = JDBC_URL_CLUSTER + "enableDirectResults=1";
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
    String jdbcUrl = JDBC_URL_CLUSTER + "enableDirectResults=1";

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
    String jdbcUrl = JDBC_URL_WAREHOUSE + "enableDirectResults=1";

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
    String jdbcUrl = JDBC_URL_CLUSTER + "enableDirectResults=1";

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
    String jdbcUrl = JDBC_URL_WAREHOUSE + "enableDirectResults=1";

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
}
