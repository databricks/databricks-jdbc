package com.databricks.jdbc.local;

import org.junit.jupiter.api.Test;

import java.sql.*;

public class DriverTester {
    public void printResultSet(ResultSet resultSet) throws SQLException {
        if (resultSet.isBeforeFirst()) {
            System.out.println("Empty result set");
            return;
        }
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        do {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println();
        } while (resultSet.next());
    }
    @Test
    void testGetTablesOSS() throws Exception {
        DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
        DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
        //Getting the connection
        String jdbcUrl = "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
        Connection con = DriverManager.getConnection(jdbcUrl, "vikrant.puppala@databricks.com", "##");
        System.out.println("Connection established......");
        //Retrieving the meta data object
        DatabaseMetaData metaData = con.getMetaData();
        //Retrieving the columns in the database
        ResultSet resultSet = metaData.getCatalogs();
        printResultSet(resultSet);
        con.close();
    }

    @Test
    void testGetTablesSimba() throws Exception {
        DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
        //Getting the connection
        String jdbcUrl = "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
        Connection con = DriverManager.getConnection(jdbcUrl, "vikrant.puppala@databricks.com", "##");
        System.out.println("Connection established......");
        //Retrieving the meta data object
        DatabaseMetaData metaData = con.getMetaData();
        System.out.println(metaData.getUserName());
        ResultSet tables = metaData.getCatalogs();
        printResultSet(tables);
        con.close();
    }

    @Test
    void testStatementSimba() throws Exception {
        DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
        //Getting the connection
        String jdbcUrl = "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
        Connection con = DriverManager.getConnection(jdbcUrl, "vikrant.puppala@databricks.com", "##");
        System.out.println("Connection established......");
        //Retrieving the meta data object
        Statement statement = con.createStatement();
        statement.setMaxRows(10);
//        DatabaseMetaData metaData = con.getMetaData();
//        System.out.println(metaData.getJDBCMinorVersion());
        ResultSet rs = statement.executeQuery("select * from samples.tpch.lineitem limit 1");
        printResultSet(rs);
        rs.close();
        statement.close();
        con.close();
    }
}
