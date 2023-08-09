package com.databricks.jdbc.local;

import com.databricks.jdbc.core.DatabricksDatabaseMetadataTest;
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
    void testGeTablesSimba() throws Exception {
        //Registering the Driver
        DriverManager.registerDriver(new com.databricks.client.jdbc.Driver());
        //Getting the connection
        String jdbcUrl = "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
        Connection con = DriverManager.getConnection(jdbcUrl, "vikrant.puppala@databricks.com", "##");
        System.out.println("Connection established......");
        //Retrieving the meta data object
        DatabaseMetaData metaData = con.getMetaData();
        String[] types = {"TABLE"};
        //Retrieving the columns in the database
        ResultSet tables = metaData.getTables(null, null, null, types);
        printResultSet(tables);
    }
    @Test
    void testGeTablesOSS() throws Exception {
        //Registering the Driver
        DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
        //Getting the connection
        String jdbcUrl = "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;";
        Connection con = DriverManager.getConnection(jdbcUrl, "vikrant.puppala@databricks.com", "##");
        System.out.println("Connection established......");
        //Retrieving the meta data object
        DatabaseMetaData metaData = con.getMetaData();
        //Retrieving the columns in the database
        ResultSet resultSet = metaData.getSchemas("samples", "*");
        printResultSet(resultSet);
    }
}
