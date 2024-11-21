package com.databricks.client.jdbc;

import static com.databricks.jdbc.integration.IntegrationTestUtil.getFullyQualifiedTableName;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.IDatabricksVolumeClient;
import com.databricks.jdbc.api.impl.DatabricksResultSetMetaData;
import com.databricks.jdbc.api.impl.arrow.ArrowResultChunk;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.sql.*;
import java.time.LocalDate;
import java.util.*;
import org.apache.http.entity.InputStreamEntity;
import org.junit.jupiter.api.Test;

public class DriverTest {
  public void printResultSet(ResultSet resultSet) throws SQLException {
    System.out.println("\n\nPrinting resultSet...........\n");
    ResultSetMetaData rsmd = resultSet.getMetaData();
    int columnsNumber = rsmd.getColumnCount();
    for (int i = 1; i <= columnsNumber; i++) System.out.print(rsmd.getColumnName(i) + "\t");
    System.out.println();
    for (int i = 1; i <= columnsNumber; i++) System.out.print(rsmd.getColumnTypeName(i) + "\t\t");
    System.out.println();
    for (int i = 1; i <= columnsNumber; i++) System.out.print(rsmd.getColumnType(i) + "\t\t\t");
    System.out.println();
    for (int i = 1; i <= columnsNumber; i++) System.out.print(rsmd.getPrecision(i) + "\t\t\t");
    System.out.println();
    while (resultSet.next()) {
      for (int i = 1; i <= columnsNumber; i++) {
        try {
          Object columnValue = resultSet.getObject(i);
          System.out.print(columnValue + "\t\t");
        } catch (Exception e) {
          System.out.print(
              "NULL\t\t"); // It is possible for certain columns to be non-existent (edge case)
        }
      }
      System.out.println();
    }
  }

  @Test
  void testGetTablesOSS_StatementExecution() throws Exception {
    DriverManager.registerDriver(new Driver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "x");
    System.out.println("Connection established......");
    Statement statement = con.createStatement();
    statement.setMaxRows(10);
    ResultSet rs = con.getMetaData().getTables("main", "%", "%", null);
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
  }

  @Test
  void testResultSetMetaData() throws Exception {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/58aa1b363649e722";

    Connection con = DriverManager.getConnection(jdbcUrl, "token", "x");
    System.out.println("Connection established with jdbc driver......");
    Statement statement = con.createStatement();
    statement.setMaxRows(10000);
    ResultSet rs =
        statement.executeQuery(
            "select * from ml.feature_store_ol_dynamodb_.test_ft_data_types LIMIT 10");
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
  }

  @Test
  void testGcpServiceAccountOauthM2M() throws Exception {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl =
        "jdbc:databricks://4371047901336987.7.gcp.databricks.com:443/default;transportMode=http;AuthMech=11;Auth_Flow=1;httpPath=/sql/1.0/warehouses/dd5955aacf3f09e5;GoogleServiceAccount=xx-compute@developer.gserviceaccount.com";

    Connection con = DriverManager.getConnection(jdbcUrl, "token", "x");
    System.out.println("Connection established with jdbc driver......");
    Statement statement = con.createStatement();
    statement.setMaxRows(10000);
    ResultSet rs = statement.executeQuery("select 1");
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
  }

  @Test
  void testGcpCredentialJsonOauthM2M() throws Exception {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl =
        "jdbc:databricks://4371047901336987.7.gcp.databricks.com:443/default;transportMode=http;AuthMech=11;Auth_Flow=1;httpPath=/sql/1.0/warehouses/dd5955aacf3f09e5;GoogleCredentialsFile=<path_to_json_credential_file>";

    Connection con = DriverManager.getConnection(jdbcUrl, "token", "x");
    System.out.println("Connection established with jdbc driver......");
    Statement statement = con.createStatement();
    statement.setMaxRows(10000);
    ResultSet rs = statement.executeQuery("select 1");
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
  }

  @Test
  void testGetTablesOSS_Metadata() throws Exception {
    DriverManager.registerDriver(new Driver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "x");
    System.out.println("Connection established......");
    ResultSet resultSet = con.getMetaData().getTables("main", "%", "%", null);
    printResultSet(resultSet);
    resultSet.close();
    con.close();
  }

  @Test
  void testGetDisposition() throws Exception {
    DriverManager.registerDriver(new Driver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con =
        DriverManager.getConnection(jdbcUrl, "token", "xx"); // Default connection, arrow enabled.
    System.out.println("Connection established with default params. Arrow is enabled ......");
    String query = "SELECT * FROM RANGE(10)";
    ResultSet resultSet = con.createStatement().executeQuery(query);
    DatabricksResultSetMetaData rsmd = (DatabricksResultSetMetaData) resultSet.getMetaData();
    System.out.println("isCloudFetchUsed when arrow is enabled: " + rsmd.getIsCloudFetchUsed());
    resultSet.close();
    con.close();
  }

  @Test
  void testArclight() throws Exception {
    DriverManager.registerDriver(new Driver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://arclight-staging-e2-arclight-dmk-qa-staging-us-east-1.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/8561171c1d9afb1f;";
    Connection con = DriverManager.getConnection(jdbcUrl, "x", "xx");
    System.out.println("Connection established......");
    // Retrieving data
    Statement statement = con.createStatement();
    statement.setMaxRows(10000);
    ResultSet rs =
        statement.executeQuery(
            "select * from `arclight-dmk-catalog`.default.samikshya_test_large_table limit 10");
    printResultSet(rs);
    System.out.println("printing is done......");
    rs.close();
    statement.close();
    con.close();
  }

  @Test
  void testThriftSqlState() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;AuthMech=3;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "xx");
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

  @Test
  void testAllPurposeClusters() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;AuthMech=3;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "xx");
    System.out.println("Connection established......");
    Statement s = con.createStatement();
    s.executeQuery("SELECT * from RANGE(5)");
    con.close();
    System.out.println("Connection closed successfully......");
  }

  @Test
  void testAllPurposeClustersInline() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;AuthMech=3;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;enableArrow=0";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "x");
    System.out.println("Connection established......");
    Statement s = con.createStatement();
    ResultSet rs = s.executeQuery("SELECT unhex('f000')");
    rs.next();
    System.out.println(Arrays.toString(rs.getBytes(1))); // should print [-16,0]
    rs = s.executeQuery("SELECT struct(1 as a, 2 as b)");
    rs.next();
    System.out.println(rs.getObject(1)); // should print {"a":1,"b":2}
    con.close();
    System.out.println("Connection closed successfully......");
  }

  @Test
  void testAllPurposeClustersMetadata() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;AuthMech=3;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "x");
    System.out.println("Connection established......");
    // ResultSet resultSet = con.getMetaData().getCatalogs();
    // ResultSet resultSet = con.getMetaData().getSchemas("main", "%");
    // ResultSet resultSet = con.getMetaData().getTables("main", "ggm_pk","table_with_pk", null);
    // ResultSet resultSet = con.getMetaData().getTables("%", "%", null, null);
    // ResultSet resultSet = con.getMetaData().getColumns("main", "ggm_pk", "%", "%");
    // ResultSet resultSet = con.getMetaData().getPrimaryKeys("main", "ggm_pk", "table_with_pk");
    ResultSet resultSet =
        con.getMetaData()
            .getFunctions("uc_1716360380283_cata", "uc_1716360380283_db1", "current_%");
    printResultSet(resultSet);
    resultSet.close();
    con.close();
  }

  @Test
  void testLogging() throws Exception {
    DriverManager.registerDriver(new Driver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;UID=token;LogLevel=debug;LogPath=./logDir;LogFileCount=3;LogFileSize=2;";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "x");
    System.out.println("Connection established......");
    ResultSet resultSet =
        con.createStatement()
            .executeQuery("SELECT * from lb_demo.demographics_fs.demographics LIMIT 10");
    printResultSet(resultSet);
    resultSet.close();
    con.close();
  }

  @Test
  void testDatatypeConversion() throws SQLException {
    DriverManager.registerDriver(new Driver());
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "x");
    System.out.println("Connection established......");
    String selectSQL =
        "SELECT id, local_date, big_integer, big_decimal FROM samikshya_catalog_2.default.test_table_2";
    ResultSet rs = con.createStatement().executeQuery(selectSQL);
    printResultSet(rs);

    LocalDate date = rs.getObject("local_date", LocalDate.class);
    System.out.println("here is date " + date + ". Class Details : " + date.getClass());

    BigInteger bigInteger = rs.getObject("big_integer", BigInteger.class);
    System.out.println(
        "here is big_integer " + bigInteger + ". Class Details : " + bigInteger.getClass());

    BigDecimal bigDecimal = rs.getObject("big_decimal", BigDecimal.class);
    System.out.println(
        "here is bigDecimal " + bigDecimal + ". Class Details : " + bigDecimal.getClass());
  }

  @Test
  void testHttpFlags() throws Exception {
    DriverManager.registerDriver(new Driver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));

    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;TemporarilyUnavailableRetry=3;";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "x");
    System.out.println("Connection established......");
    con.close();
  }

  @Test
  void testUCVolumeUsingInputStream() throws Exception {
    DriverManager.registerDriver(new Driver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    System.out.println("Starting test");
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "xx");
    con.setClientInfo(DatabricksJdbcConstants.ALLOWED_VOLUME_INGESTION_PATHS, "delete");
    System.out.println("Connection created");
    IDatabricksVolumeClient client = ((IDatabricksConnection) con).getVolumeClient();

    File file = new File("/tmp/put.txt");
    try {
      Files.writeString(file.toPath(), "test-put");

      System.out.println("File created");

      System.out.println(
          "Object inserted "
              + client.putObject(
                  "samikshya_hackathon",
                  "default",
                  "gopal-psl",
                  "test-stream.csv",
                  new FileInputStream(file),
                  file.length(),
                  true));

      InputStreamEntity inputStream =
          client.getObject("samikshya_hackathon", "default", "gopal-psl", "test-stream.csv");
      System.out.println("Got data " + new String(inputStream.getContent().readAllBytes()));
      inputStream.getContent().close();

      System.out.println(
          "Object exists "
              + client.objectExists(
                  "samikshya_hackathon", "default", "gopal-psl", "test-stream.csv", false));
      client.deleteObject("samikshya_hackathon", "default", "gopal-psl", "test-stream.csv");
      System.out.println(
          "Object exists "
              + client.objectExists(
                  "samikshya_hackathon", "default", "gopal-psl", "test-stream.csv", false));
    } finally {
      file.delete();
      con.close();
    }
  }

  @Test
  void testDBFSVolumeOperation() throws Exception {
    DriverManager.registerDriver(new Driver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    System.out.println("Starting test");
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/dd43ee29fedd958d;Loglevel=debug;useFileSystemAPI=1";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "xx");
    System.out.println("Connection created");

    IDatabricksVolumeClient client = ((IDatabricksConnection) con).getVolumeClient();

    File file = new File("/tmp/put.txt");
    try {
      Files.writeString(file.toPath(), "put string check");
      System.out.println("File created");

      System.out.println(
          "Object inserted "
              + client.putObject(
                  "___________________first",
                  "jprakash-test",
                  "jprakash_volume",
                  "test-stream.csv",
                  "/tmp/put.txt",
                  true));

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      file.delete();
      con.close();
    }
  }

  @Test
  public void tooManyParameters() throws SQLException {
    DriverManager.registerDriver(new Driver());
    StringBuilder sql =
        new StringBuilder("SELECT * FROM lb_demo.demographics_fs.demographics WHERE age IN (");
    StringJoiner joiner = new StringJoiner(",");
    for (int i = 0; i < 300; i++) {
      joiner.add("?");
    }
    sql.append(joiner).append(")");
    System.out.println("here is SQL " + sql);
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;supportManyParameters=1";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "x");
    PreparedStatement pstmt = con.prepareStatement(sql.toString());
    List<Integer> ids = new ArrayList<>();
    for (int i = 1; i <= 300; i++) {
      ids.add(i);
    }
    for (int i = 0; i < ids.size(); i++) {
      pstmt.setInt(i + 1, ids.get(i));
    }
    ResultSet rs = pstmt.executeQuery();
    printResultSet(rs);
  }

  @Test
  void testAllPurposeClusters_errorHandling() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;AuthMech=3;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;enableDirectResults=1";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "xx");
    System.out.println("Connection established......");
    Statement s = con.createStatement();
    s.executeQuery("SELECT * from RANGE(10)");
    con.close();
    System.out.println("Connection closed successfully......");
  }

  @Test
  void testAllPurposeClusters_async() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;AuthMech=3;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;enableDirectResults=1";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "token");
    System.out.println("Connection established...... con1");
    Statement s = con.createStatement();
    IDatabricksStatement ids = s.unwrap(IDatabricksStatement.class);
    ResultSet rs = ids.executeAsync("SELECT * from RANGE(10)");
    System.out.println(
        "1Status of async execution " + rs.unwrap(IDatabricksResultSet.class).getStatementStatus());

    ResultSet rs3 = s.unwrap(IDatabricksStatement.class).getExecutionResult();
    System.out.println(
        "2Status of async execution "
            + rs3.unwrap(IDatabricksResultSet.class).getStatementStatus());

    System.out.println("StatementId " + rs.unwrap(IDatabricksResultSet.class).getStatementId());

    Connection con2 = DriverManager.getConnection(jdbcUrl, "token", "token");
    System.out.println("Connection established......con2");
    IDatabricksConnection idc = con2.unwrap(IDatabricksConnection.class);
    Statement stm = idc.getStatement(rs.unwrap(IDatabricksResultSet.class).getStatementId());
    ResultSet rs2 = stm.unwrap(IDatabricksStatement.class).getExecutionResult();
    System.out.println(
        "3Status of async execution "
            + rs2.unwrap(IDatabricksResultSet.class).getStatementStatus());
    stm.cancel();
    System.out.println("Statement cancelled using con2");
    s.close();
    System.out.println("Statement cancelled using con1");
    con2.close();
    con.close();
    System.out.println("Connection closed successfully......");
  }

  @Test
  void testBatchFunction() throws Exception {

    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/dd43ee29fedd958d;";
    Connection con = DriverManager.getConnection(jdbcUrl, "jothi.prakash@databricks.com", "xx");
    System.out.println("Connection established......");

    //
    // Batch Statement Testing
    //
    String sqlStatement =
        "INSERT INTO ___________________first.`jprakash-test`.diamonds (carat, cut, color, clarity) VALUES (?, ?, ?, ?)";
    PreparedStatement pstmt = con.prepareStatement(sqlStatement);
    for (int i = 1; i <= 3; i++) {
      pstmt.setFloat(1, 0.23f);
      pstmt.setString(2, "OK");
      pstmt.setString(3, "E");
      pstmt.setString(4, "SI2");
      pstmt.addBatch();
    }

    pstmt.setString(1, "Shaama");
    pstmt.setString(2, "Bad");
    pstmt.setString(3, "F");
    pstmt.setString(4, "SI6");
    pstmt.addBatch();

    for (int i = 1; i <= 3; i++) {
      pstmt.setFloat(1, 0.23f);
      pstmt.setString(2, "Bad");
      pstmt.setString(3, "F");
      pstmt.setString(4, "SI6");
      pstmt.addBatch();
    }

    // Execute the batch
    int[] updateCounts = pstmt.executeBatch();

    // Process the update counts
    for (int count : updateCounts) {
      System.out.println("Update count: " + count);
    }
    con.close();
  }

  @Test
  void testM2MJWT() throws SQLException {
    String jdbcUrl =
        "jdbc:databricks://mkazia-pl-sandbox.staging.cloud.databricks.com:443/default;"
            + "httpPath=sql/1.0/warehouses/31e4555776d18496;"
            + "AuthMech=11;ssl=1;Auth_Flow=1;"
            + "OAuth2TokenEndpoint=https://dev-591123.oktapreview.com/oauth2/aus1mzu4zk5TWwMvx0h8/v1/token;"
            + "Auth_Scope=sql;OAuth2ClientId=0oa25wnir4ehnKDj10h8;"
            + "Auth_KID=EbKQzTAVP1_3E59Bq5P3Uv8krHCpj3hIWTodcmDwQ5k;"
            + "UseJWTAssertion=1;"
            + "Auth_JWT_Key_File=jdbc-testing-enc.pem;"
            + "Auth_JWT_Key_Passphrase=s3cr3t";
    Connection con = DriverManager.getConnection(jdbcUrl);
    System.out.println("Connection established......");
    ResultSet rs = con.createStatement().executeQuery("SELECT 1");
    printResultSet(rs);
    con.close();
  }

  @Test
  void testChunkDownloadRetry() throws Exception {
    // Enable error injection
    ArrowResultChunk.enableErrorInjection();
    ArrowResultChunk.setErrorInjectionCountMaxValue(2);
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/58aa1b363649e722";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "xx");
    System.out.println("Connection established......");
    Statement s = con.createStatement();
    s.executeQuery("SELECT * from RANGE(37500000)");
    printResultSet(s.getResultSet());
    con.close();
    System.out.println("Connection closed successfully......");
    // Disable error injection after the test (not strictly needed as test launches a new JVM)
    ArrowResultChunk.disableErrorInjection();
  }

  @Test
  void testBatchAllPurposeClusters() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;AuthMech=3;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;MaxBatchSize=4";
    String tableName = "batch_test_table";
    Connection con = DriverManager.getConnection(jdbcUrl, "token", "xx");
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
    System.out.println(Arrays.toString(s.executeBatch()));
    s.clearBatch();
    con.close();
    System.out.println("Connection closed successfully......");
  }
}
