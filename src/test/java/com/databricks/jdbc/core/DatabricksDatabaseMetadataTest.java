package com.databricks.jdbc.core;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.*;


public class DatabricksDatabaseMetadataTest {
    private static Connection connection;

    @BeforeAll
    public static void connect() throws SQLException {
        //Registering the Driver
        DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
        //Getting the connection
        String jdbcUrl = "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
        connection = DriverManager.getConnection(jdbcUrl, "vikrant.puppala@databricks.com", "##");
        System.out.println("Connection established......");
    }
    @Test
    public void testGetCatalogs() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        //Retrieving the columns in the database
        ResultSet resultSet = metaData.getCatalogs();
        printResultSet(resultSet);
    }

    @Test void testGetSchemas() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        //Retrieving the columns in the database
        ResultSet resultSet = metaData.getSchemas("samples", "*");
        printResultSet(resultSet);
    }

    @Test void testGetTables() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        //Retrieving the columns in the database
        ResultSet resultSet = metaData.getTables("samples", "tpch", "line*", new String[0]);
        printResultSet(resultSet);
    }

    public void printResultSet(ResultSet resultSet) throws SQLException {
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
}
