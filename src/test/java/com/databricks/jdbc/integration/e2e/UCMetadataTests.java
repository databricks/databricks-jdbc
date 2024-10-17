package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.*;

public class UCMetadataTests {

  private static Connection connection;

  static long currentTimeMillis = System.currentTimeMillis();
  static String prefix = "uc_" + currentTimeMillis + "_"; // Define your prefix here
  static String catA = prefix + "catA";
  static String hiveCatalog = "hive_metastore"; // Define your hive catalog here

  String sparkCatalog = "spark";

  static String mainCatalog = "main";
  static String db1 = prefix + "db1";
  static String db2 = prefix + "db2";
  static String table1 = prefix + "table_1";
  static String table2 = prefix + "table_2";
  static String mainDb1Table1 = prefix + "main_1";

  @BeforeAll
  static void setUp() throws SQLException {
    // Change connection to actual test warehouse once it supports latest runtime
    connection = getDogfoodJDBCConnection(List.of(List.of("useLegacyMetadata", "0")));

    executeSQL(connection, "CREATE CATALOG IF NOT EXISTS " + catA);
    executeSQL(connection, "USE CATALOG " + catA);
    executeSQL(connection, "CREATE DATABASE IF NOT EXISTS " + db1);
    executeSQL(connection, "CREATE DATABASE IF NOT EXISTS " + db2);

    executeSQL(connection, "USE " + db1);
    executeSQL(
        connection,
        "CREATE TABLE IF NOT EXISTS a_1 AS (SELECT 1 AS col_1, '"
            + catA
            + "."
            + db1
            + ".a_1' AS col_2)");
    executeSQL(
        connection,
        "CREATE TABLE IF NOT EXISTS a_2 AS (SELECT 1 AS col_1, '"
            + catA
            + "."
            + db1
            + ".a_2' AS col_2)");

    executeSQL(connection, "USE " + db2);
    executeSQL(
        connection,
        "CREATE TABLE IF NOT EXISTS a_1 AS (SELECT 1 AS col_1, '"
            + catA
            + "."
            + db2
            + ".a_1' AS col_2)");

    executeSQL(connection, "USE CATALOG " + mainCatalog);
    executeSQL(connection, "CREATE DATABASE IF NOT EXISTS " + db1);
    executeSQL(connection, "CREATE DATABASE IF NOT EXISTS " + db2);

    executeSQL(connection, "USE " + db1);
    executeSQL(
        connection,
        "CREATE TABLE IF NOT EXISTS "
            + mainDb1Table1
            + " AS (SELECT 1 AS col_1, 'main."
            + db1
            + "."
            + mainDb1Table1
            + "' AS col_2)");

    executeSQL(connection, "USE CATALOG " + hiveCatalog);
    executeSQL(connection, "CREATE DATABASE IF NOT EXISTS " + db2);
    executeSQL(connection, "USE " + db2);
    executeSQL(
        connection,
        "CREATE TABLE IF NOT EXISTS "
            + table1
            + " AS (SELECT 1 AS col_1, '"
            + hiveCatalog
            + "."
            + db2
            + "."
            + table1
            + "' AS col_2)");
    executeSQL(
        connection,
        "CREATE TABLE IF NOT EXISTS "
            + table2
            + " AS (SELECT 1 AS col_1, '"
            + hiveCatalog
            + "."
            + db2
            + "."
            + table2
            + "' AS col_2)");

    // Created tables:
    // |     catalog    |    schema   |        table       |
    // +----------------+-------------+--------------------+
    // |      cat_a     |     db1     |         a_1        |
    // |      cat_a     |     db1     |         a_2        |
    // |      cat_a     |     db2     |         a_1        |
    // |      main      |     db1     |       main_1       |
    // |      main      |     db2     |          ∅         |
    // | hive_metastore |     db2     |  hive_metastore_1  |
    // | hive_metastore |     db2     |  hive_metastore_2  |
  }

  @AfterAll
  static void cleanUp() throws SQLException {
    // Cleanup
    executeSQL(connection, "USE CATALOG " + catA);
    executeSQL(connection, "DROP DATABASE " + db1 + " CASCADE");
    executeSQL(connection, "DROP DATABASE " + db2 + " CASCADE");
    executeSQL(connection, "DROP DATABASE IF EXISTS default CASCADE");
    executeSQL(connection, "DROP CATALOG " + catA);

    // Cleanup main catalog
    executeSQL(connection, "USE CATALOG " + mainCatalog);
    executeSQL(connection, "DROP DATABASE " + db1 + " CASCADE");
    executeSQL(connection, "DROP DATABASE " + db2 + " CASCADE");

    // Cleanup legacy catalog
    executeSQL(connection, "USE CATALOG " + hiveCatalog);
    executeSQL(connection, "DROP DATABASE " + db2 + " CASCADE");

    if (connection != null) {
      connection.close();
    }
  }

  @Test
  void testGetCatalogs() throws SQLException {
    ResultSet r = connection.getMetaData().getCatalogs();
    verifyContainsCatalogs(r, List.of("main", "hive_metastore", "samples", catA.toLowerCase()));
  }

  @Test
  void testGetSchemas() throws SQLException {
    executeSQL(connection, "USE CATALOG hive_metastore");
    ResultSet r = connection.getMetaData().getSchemas("hive_metastore", "%");
    verifyContainsSchemas(
        r,
        List.of(
            List.of("hive_metastore", "default"), List.of("hive_metastore", db2.toLowerCase())));

    r = connection.getMetaData().getSchemas(catA.toLowerCase(), "%");
    verifyContainsSchemas(
        r,
        List.of(
            List.of(catA.toLowerCase(), db1.toLowerCase()),
            List.of(catA.toLowerCase(), db2.toLowerCase()),
            List.of(catA.toLowerCase(), "default")));
  }

  @Test
  void testGetTables() throws SQLException {
    ResultSet r = connection.getMetaData().getTables(catA.toLowerCase(), "%", "%", null);
    verifyContainsTables(
        r,
        List.of(
            List.of(catA.toLowerCase(), db1.toLowerCase(), "a_1", "TABLE"),
            List.of(catA.toLowerCase(), db1.toLowerCase(), "a_2", "TABLE"),
            List.of(catA.toLowerCase(), db2.toLowerCase(), "a_1", "TABLE")));
  }

  @Test
  void testGetColumns() throws SQLException {
    ResultSet r = connection.getMetaData().getColumns(catA.toLowerCase(), "%", "%", "%");
    verifyContainsColumns(
        r,
        List.of(
            List.of(catA.toLowerCase(), db1.toLowerCase(), "a_1", "col_1", "INT"),
            List.of(catA.toLowerCase(), db1.toLowerCase(), "a_1", "col_2", "STRING"),
            List.of(catA.toLowerCase(), db1.toLowerCase(), "a_2", "col_1", "INT"),
            List.of(catA.toLowerCase(), db1.toLowerCase(), "a_2", "col_2", "STRING"),
            List.of(catA.toLowerCase(), db2.toLowerCase(), "a_1", "col_1", "INT"),
            List.of(catA.toLowerCase(), db2.toLowerCase(), "a_1", "col_2", "STRING")));
  }

  @Test
  void testGetCurrentCatalogAndSchema() throws SQLException {
    connection =
        getDogfoodJDBCConnection(
            List.of(List.of("useLegacyMetadata", "0"), List.of("connCatalog", catA)));
    ResultSet r =
        connection.createStatement().executeQuery("SELECT current_catalog(), current_database()");
    r.next();
    assert (r.getString(1).equals(catA.toLowerCase()));
    assert (r.getString(2).equals("default"));

    connection =
        getDogfoodJDBCConnection(
            List.of(List.of("useLegacyMetadata", "0"), List.of("connCatalog", "samples")));
    r = connection.createStatement().executeQuery("SELECT current_catalog(), current_database()");
    r.next();
    assert (r.getString(1).equals("samples"));
    assert (r.getString(2).equals("default"));

    connection =
        getDogfoodJDBCConnection(
            List.of(List.of("useLegacyMetadata", "0"), List.of("connSchema", db2.toLowerCase())));
    r = connection.createStatement().executeQuery("SELECT current_catalog(), current_database()");
    r.next();
    assert (r.getString(1).equals(sparkCatalog));
    assert (r.getString(2).equals(db2.toLowerCase()));

    connection =
        getDogfoodJDBCConnection(
            List.of(
                List.of("useLegacyMetadata", "0"),
                List.of("connCatalog", "fake_catalog"),
                List.of("connSchema", "fake_schema")));
    r = connection.createStatement().executeQuery("SELECT current_catalog(), current_database()");
    r.next();
    assert (r.getString(1).equals("fake_catalog"));
    assert (r.getString(2).equals("fake_schema"));
  }

  private void verifyContainsColumns(ResultSet r, List<List<String>> included_columns) {
    ArrayList<List<String>> allColumns = new ArrayList<>();
    try {
      while (r.next()) {
        allColumns.add(
            List.of(
                r.getString("TABLE_CAT"),
                r.getString("TABLE_SCHEM"),
                r.getString("TABLE_NAME"),
                r.getString("COLUMN_NAME"),
                r.getString("DATA_TYPE")));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    assert (allColumns.containsAll(included_columns));
  }

  private void verifyContainsTables(ResultSet r, List<List<String>> included_tables) {
    ArrayList<List<String>> allTables = new ArrayList<>();
    try {
      while (r.next()) {
        allTables.add(
            List.of(
                r.getString("TABLE_CAT"),
                r.getString("TABLE_SCHEM"),
                r.getString("TABLE_NAME"),
                r.getString("TABLE_TYPE")));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    assert (allTables.containsAll(included_tables));
  }

  private void verifyContainsSchemas(ResultSet r, List<List<String>> included_schema)
      throws SQLException {
    ArrayList<List<String>> allSchemas = new ArrayList<>();
    try {
      while (r.next()) {
        allSchemas.add(List.of(r.getString("TABLE_CAT"), r.getString("TABLE_SCHEM")));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    assert (allSchemas.containsAll(included_schema));
  }

  private void verifyContainsCatalogs(ResultSet r, List<String> included_catalogs) {
    ArrayList<String> allCatalogs = new ArrayList<>();
    try {
      while (r.next()) {
        allCatalogs.add(r.getString("TABLE_CAT"));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    assert (allCatalogs.containsAll(included_catalogs));
  }
}
