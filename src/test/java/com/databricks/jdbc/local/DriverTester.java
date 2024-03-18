package com.databricks.jdbc.local;

import java.sql.*;
import org.junit.jupiter.api.Test;

public class DriverTester {
  public void printResultSet(ResultSet resultSet) throws SQLException {
    ResultSetMetaData rsmd = resultSet.getMetaData();
    int columnsNumber = rsmd.getColumnCount();
    for (int i = 1; i <= columnsNumber; i++) System.out.print(rsmd.getColumnName(i) + "\t\t\t");
    System.out.println();
    for (int i = 1; i <= columnsNumber; i++) System.out.print(rsmd.getColumnTypeName(i) + "\t\t\t");
    System.out.println();
    for (int i = 1; i <= columnsNumber; i++) System.out.print(rsmd.getColumnType(i) + "\t\t\t");
    System.out.println();
    for (int i = 1; i <= columnsNumber; i++) System.out.print(rsmd.getPrecision(i) + "\t\t\t");
    System.out.println();
    while (resultSet.next()) {
      for (int i = 1; i <= columnsNumber; i++) {
        Object columnValue = resultSet.getObject(i);
        System.out.print(columnValue + "\t");
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
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
    System.out.println("Connection established......");
    Statement statement = con.createStatement();
    statement.setMaxRows(10);
    ResultSet rs = statement.executeQuery("select * from samples.tpch.lineitem limit 10");
    // ResultSet rs = statement.executeQuery("select 1");
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
  }

  @Test
  void testGetTablesOSS_UC_Test_PUT() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
    con.setCatalog("samikshya_hackathon");
    con.setSchema("default");
    System.out.println("Connection established......");
    Statement statement = con.createStatement();
    statement.setMaxRows(10);
    ResultSet rs =
        statement.executeQuery(
            "PUT '/Users/samikshya.chand/Desktop/1.csv' INTO '/Volumes/samikshya_hackathon/default/newvol/helloNewFile.csv' OVERWRITE");
    // ResultSet rs = statement.executeQuery("select 1");
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
  }

  @Test
  void testOSS_UC_Test_GET() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
    con.setCatalog("samikshya_hackathon");
    con.setSchema("default");
    System.out.println("Connection established......");
    Statement statement = con.createStatement();
    statement.setMaxRows(10);
    ResultSet rs =
        statement.executeQuery(
            "GET '/Volumes/samikshya_hackathon/default/newvol/1.csv' TO '/Users/samikshya.chand/Desktop/1hi222'");
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
  }

  @Test
  void testOSS_UC_Test_CREATE_TABLE_using_volume() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
    con.setCatalog("samikshya_hackathon");
    con.setSchema("default");
    System.out.println("Connection established......");
    Statement statement = con.createStatement();
    statement.setMaxRows(10);
    ResultSet rs =
        statement.executeQuery(
            "CREATE TABLE test_table  AS SELECT * FROM read_files('/Volumes/samikshya_hackathon/default/newvol/1.csv')");
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
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "xx");
    System.out.println("Connection established......");
    DatabaseMetaData metaData = con.getMetaData();
    ResultSet resultSet = metaData.getTables("samples", "tpch", null, null);
    printResultSet(resultSet);
    resultSet.close();
    con.close();
  }

  @Test
  void testAllPurposeTest_PUT() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;UID=token;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
    System.out.println("Connection established......");
    Statement statement = con.createStatement();
    ResultSet rs =
        statement.executeQuery(
            "PUT '/Users/samikshya.chand/Desktop/1.csv' INTO '/Volumes/samikshya_hackathon/default/newvol/helloNewFile.csv' OVERWRITE");
    printResultSet(rs);
    con.close();
    System.out.println("Connection closed successfully......");
  }

  @Test
  void testAllPurposeTest_GET() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;UID=token;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
    System.out.println("Connection established......");
    Statement statement = con.createStatement();
    ResultSet rs =
        statement.executeQuery(
            "GET '/Volumes/samikshya_hackathon/default/newvol/1.csv' TO '/Users/samikshya.chand/Desktop/helloNewFile.csv'");
    printResultSet(rs);
    con.close();
    System.out.println("Connection closed successfully......");
  }

  @Test
  void testAllPurposeMetadata() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;UID=token;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
    ResultSet rs =
        con.getMetaData().getPrimaryKeys("adriana_ispas", "default", "samikshya_test_table");
    // System.out.println("Here you go "+rs.);
    // ResultSet rs =con.getMetaData().getSchemas("adriana_ispas","quickstart"); -> NOT WORKING
    // ResultSet rs =con.getMetaData().getCatalogs();
    // ResultSet rs = con.getMetaData().getTables("adriana_ispas","","", null);
    // ResultSet rs =con.getMetaData().getFunctions("adriana_ispas", "default", "*");
    printResultSet(rs);
  }

  /*
  @Test
  void testGetTablesSimba() throws Exception {
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "madhav.sainanee@databricks.com", "##");
    System.out.println("Connection established......");
    // Retrieving the meta data object
    DatabaseMetaData metaData = con.getMetaData();
    System.out.println(con.getAutoCommit());
    ResultSet rs = metaData.getSchemas();
    printResultSet(rs);
    con.close();
  }*/

  /*
  @Test
  void testStatementSimba() throws Exception {
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "xx");
    System.out.println("Connection established......");
    // Retrieving the meta data object
    Statement statement = con.createStatement();
    statement.setMaxRows(10);
    ResultSet rs = statement.executeQuery("select * from samples.tpch.lineitem limit 10");
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
  }*/

  @Test
  void testArclight() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://arclight-staging-e2-arclight-dmk-qa-staging-us-east-1.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/8561171c1d9afb1f;";
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
}
