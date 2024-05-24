package com.databricks.jdbc.local;

import java.sql.*;
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
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
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
  void testGetTablesOSS_Metadata() throws Exception {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    // Getting the connection
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
    System.out.println("Connection established......");
    DatabaseMetaData metaData = con.getMetaData();
    ResultSet resultSet = metaData.getCatalogs();
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
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;UID=token;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "x");
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
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;UID=token;";
    Connection con = DriverManager.getConnection(jdbcUrl, "samikshya.chand@databricks.com", "xx");
    System.out.println("Connection established......");
    ResultSet resultSet = con.getMetaData().getPrimaryKeys("main", "ggm_pk", "table_with_pk");
    printResultSet(resultSet);
    resultSet.close();
    con.close();
  }
}
