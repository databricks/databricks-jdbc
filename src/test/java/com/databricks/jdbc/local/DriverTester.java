package com.databricks.jdbc.local;

import java.sql.*;
import org.junit.jupiter.api.Test;

public class DriverTester {
  public void printResultSet(ResultSet resultSet) throws SQLException {
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
        Object columnValue = resultSet.getObject(i);
        System.out.print(columnValue + "\t\t");
      }
      System.out.println();
    }
  }

  @Test
  void testGetTablesOSS() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con =
        DriverManager.getConnection(
            jdbcUrl, "samikshya.chand@databricks.com", "dapi4e0f1f5184ac01978969f44e94267bbf");
    System.out.println("Connection established......");
    Statement statement = con.createStatement();
    statement.setMaxRows(10);
    ResultSet rs = statement.executeQuery("select * from samples.tpch.lineitem limit 10");
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
  }

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
  }

  @Test
  void testStatementSimba() throws Exception {
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;";
    Connection con =
        DriverManager.getConnection(
            jdbcUrl, "samikshya.chand@databricks.com", "dapi4e0f1f5184ac01978969f44e94267bbf");
    System.out.println("Connection established......");
    // Retrieving the meta data object
    Statement statement = con.createStatement();
    statement.setMaxRows(10);
    ResultSet rs = statement.executeQuery("select * from samples.tpch.lineitem limit 10");
    printResultSet(rs);
    rs.close();
    statement.close();
    con.close();
  }

  @Test
  void testArclight() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://arclight-staging-e2-arclight-dmk-qa-staging-us-east-1.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/8561171c1d9afb1f;";
    Connection con =
        DriverManager.getConnection(
            jdbcUrl, "yunbo.deng+arclight+dmk+staging@databricks.com", "xx");
    System.out.println("Connection established......");
    // Retrieving data
    Statement statement = con.createStatement();
    statement.setMaxRows(10000);
    ResultSet rs =
        statement.executeQuery(
            "select * from `arclight-dmk-catalog`.default.samikshya_test_large_table");
    printResultSet(rs);
    System.out.println("printing is done......");
    rs.close();
    statement.close();
    con.close();
  }
}
