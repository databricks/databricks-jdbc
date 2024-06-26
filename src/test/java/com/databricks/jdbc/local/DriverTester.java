package com.databricks.jdbc.local;

import com.databricks.jdbc.commons.MetricsList;
import com.databricks.jdbc.telemetry.DatabricksMetrics;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class DriverTester {
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
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "xx");
    System.out.println("Connection established......");
    Statement statement = con.createStatement();
    statement.setMaxRows(10);
    ResultSet rs = con.getMetaData().getTables("hive_metastore", "*", "*", null);
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
  }

  @Test
  void testGetTablesOSS_Metadata() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
    System.out.println("Connection established......");
    // DatabaseMetaData metaData = con.getMetaData();
    ResultSet resultSet = con.getMetaData().getTables("main", ".*", ".*", null);
    printResultSet(resultSet);
    resultSet.close();
    con.close();
  }

  @Test
  void testArclight() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://arclight-staging-e2-arclight-dmk-qa-staging-us-east-1.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/8561171c1d9afb1f;";
    Connection con = DriverManager.getConnection(jdbcUrl, "yunbo.deng@databricks.com", "xx");
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
  void testAllPurposeClusters() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;AuthMech=3;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
    DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
    System.out.println("Connection established......");
    Statement statement = con.createStatement();
    ResultSet rs =
        statement.executeQuery("SELECT * from lb_demo.demographics_fs.demographics LIMIT 10");
    printResultSet(rs);
    con.close();
    System.out.println("Connection closed successfully......");
  }

  @Test
  void testAllPurposeClustersMetadata() throws Exception {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;AuthMech=3;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
    System.out.println("Connection established......");
    // ResultSet resultSet = con.getMetaData().getCatalogs();
    // ResultSet resultSet = con.getMetaData().getSchemas("main", "%");
    // ResultSet resultSet = con.getMetaData().getTables("main", "ggm_pk","table_with_pk", null);
    // ResultSet resultSet = con.getMetaData().getTables("%", "%", null, null);
    ResultSet resultSet = con.getMetaData().getColumns("main", "ggm_pk", "%", "%");
    // con.getMetaData().getPrimaryKeys("main", "ggm_pk", "table_with_pk");
    printResultSet(resultSet);
    resultSet.close();
    con.close();
  }

  @Test
  void testLogging() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;UID=token;LogLevel=debug;LogPath=test;LogFileCount=3;LogFileSize=2;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
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
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
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
  void testModifyMetrics() throws Exception {
    for (int i = 1; i <= 10; i++) {
      DatabricksMetrics.record(MetricsList.LIST_TABLES_METADATA_SEA.name(), (2 * i));
      DatabricksMetrics.record(MetricsList.LIST_PRIMARY_KEYS_METADATA_SEA.name(), i ^ 2);
      Thread.sleep(1000);
    }
    Thread.sleep(5000);

    for (int i = 1; i <= 10; i++) {
      DatabricksMetrics.record(MetricsList.LIST_TABLES_METADATA_SEA.name(), (3 * i));
      DatabricksMetrics.record(MetricsList.LIST_PRIMARY_KEYS_METADATA_SEA.name(), i ^ 3);
      Thread.sleep(1000);
    }

    System.out.println(DatabricksMetrics.getHttpLatency());
  }

  @Test
  public void testExecuteHttpRequest() throws Exception {

    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));

    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
    System.out.println("Connection established......");

    Map<String, Double> map = new HashMap<>();
    map.put("M1", (double) 5);
    map.put("M2", (double) 7);

    String response = DatabricksMetrics.sendRequest(map);
    System.out.println(response);
  }

  @Test
  void testHttpFlags() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));

    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;TemporarilyUnavailableRetry=3;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
    System.out.println("Connection established......");
    con.close();
  }

  @Test
  void testGetMetadataTypeBasedOnDBSQLVersion() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;uselegacyMetadata=1";
    Connection con = DriverManager.getConnection(jdbcUrl, "madhav", "xx");
    System.out.println("Connection established......");
    con.close();
  }
}
