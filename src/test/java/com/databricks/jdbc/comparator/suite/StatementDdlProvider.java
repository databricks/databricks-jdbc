package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Compares DDL operations (CREATE, ALTER, DROP) between Thrift and SEA connections.
 *
 * <p>Runs as a single ordered flow: CREATE SCHEMA → CREATE TABLE → ALTER TABLE → DROP TABLE → DROP
 * SCHEMA. Each connection operates on its own namespace (suffixed _thrift / _sea) inside the
 * comparator_ddl_tests catalog. Compares executeUpdate return values, exception behavior, and side
 * effects.
 */
public class StatementDdlProvider implements SuiteProvider {

  private static final String CATALOG = "comparator_ddl_tests";

  @Override
  public List<TestCase> getTestCases() {
    return Collections.singletonList(
        new TestCase(
            "DDL_FLOW",
            "DDL flow: CREATE SCHEMA → CREATE TABLE → ALTER → DROP TABLE → DROP SCHEMA"));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String schema1 = CATALOG + ".ddl_thrift";
    String schema2 = CATALOG + ".ddl_sea";
    String table1 = schema1 + ".test_table";
    String table2 = schema2 + ".test_table";

    List<String> differences = new ArrayList<>();

    // Clean slate
    exec(conn1, "DROP SCHEMA IF EXISTS " + schema1 + " CASCADE");
    exec(conn2, "DROP SCHEMA IF EXISTS " + schema2 + " CASCADE");

    // 1. CREATE SCHEMA
    compareDdl(
        conn1,
        conn2,
        "CREATE SCHEMA " + schema1,
        "CREATE SCHEMA " + schema2,
        differences,
        "CREATE SCHEMA");
    verifySideEffect(
        conn1,
        conn2,
        "SHOW SCHEMAS IN " + CATALOG + " LIKE 'ddl_thrift'",
        "SHOW SCHEMAS IN " + CATALOG + " LIKE 'ddl_sea'",
        1,
        differences,
        "schema exists after CREATE");

    // 2. CREATE TABLE
    compareDdl(
        conn1,
        conn2,
        "CREATE TABLE " + table1 + " (id INT, name STRING, value DOUBLE, active BOOLEAN)",
        "CREATE TABLE " + table2 + " (id INT, name STRING, value DOUBLE, active BOOLEAN)",
        differences,
        "CREATE TABLE");
    verifySideEffect(
        conn1,
        conn2,
        "DESCRIBE TABLE " + table1,
        "DESCRIBE TABLE " + table2,
        4,
        differences,
        "table has 4 columns after CREATE");

    // 3. ALTER TABLE ADD COLUMN
    compareDdl(
        conn1,
        conn2,
        "ALTER TABLE " + table1 + " ADD COLUMNS (extra_col STRING)",
        "ALTER TABLE " + table2 + " ADD COLUMNS (extra_col STRING)",
        differences,
        "ALTER TABLE ADD COLUMN");
    verifySideEffect(
        conn1,
        conn2,
        "DESCRIBE TABLE " + table1,
        "DESCRIBE TABLE " + table2,
        5,
        differences,
        "table has 5 columns after ALTER");

    // 4. DROP TABLE
    compareDdl(
        conn1, conn2, "DROP TABLE " + table1, "DROP TABLE " + table2, differences, "DROP TABLE");
    verifySideEffect(
        conn1,
        conn2,
        "SHOW TABLES IN " + schema1 + " LIKE 'test_table'",
        "SHOW TABLES IN " + schema2 + " LIKE 'test_table'",
        0,
        differences,
        "table gone after DROP");

    // 5. DROP SCHEMA
    compareDdl(
        conn1,
        conn2,
        "DROP SCHEMA " + schema1,
        "DROP SCHEMA " + schema2,
        differences,
        "DROP SCHEMA");
    verifySideEffect(
        conn1,
        conn2,
        "SHOW SCHEMAS IN " + CATALOG + " LIKE 'ddl_thrift'",
        "SHOW SCHEMAS IN " + CATALOG + " LIKE 'ddl_sea'",
        0,
        differences,
        "schema gone after DROP");

    ComparisonResult result = new ComparisonResult(label, "DDL_FLOW", testCase.getArgs());
    result.dataDifferences = differences;
    return result;
  }

  /** Executes DDL on both connections and compares return values and exceptions. */
  private void compareDdl(
      Connection conn1,
      Connection conn2,
      String sql1,
      String sql2,
      List<String> differences,
      String operation) {
    Object result1 = executeDdl(conn1, sql1);
    Object result2 = executeDdl(conn2, sql2);

    if (result1 instanceof Exception && result2 instanceof Exception) {
      String type1 = result1.getClass().getSimpleName();
      String type2 = result2.getClass().getSimpleName();
      if (!type1.equals(type2)) {
        differences.add(operation + ": exception type mismatch: " + type1 + " vs " + type2);
      }
    } else if (result1 instanceof Exception) {
      differences.add(
          operation
              + ": Thrift threw "
              + result1.getClass().getSimpleName()
              + " but SEA succeeded");
    } else if (result2 instanceof Exception) {
      differences.add(
          operation + ": Thrift succeeded but SEA threw " + result2.getClass().getSimpleName());
    } else {
      if (!result1.equals(result2)) {
        differences.add(operation + ": return value mismatch: " + result1 + " vs " + result2);
      }
    }
  }

  private Object executeDdl(Connection conn, String sql) {
    try (Statement stmt = conn.createStatement()) {
      return stmt.executeUpdate(sql);
    } catch (Exception e) {
      return e;
    }
  }

  private void verifySideEffect(
      Connection conn1,
      Connection conn2,
      String sql1,
      String sql2,
      int expectedRows,
      List<String> differences,
      String context) {
    int count1 = countRows(conn1, sql1);
    int count2 = countRows(conn2, sql2);
    if (count1 != expectedRows) {
      differences.add(context + ": Thrift expected " + expectedRows + " rows, got " + count1);
    }
    if (count2 != expectedRows) {
      differences.add(context + ": SEA expected " + expectedRows + " rows, got " + count2);
    }
    if (count1 != count2) {
      differences.add(context + ": row count mismatch: Thrift=" + count1 + " vs SEA=" + count2);
    }
  }

  private int countRows(Connection conn, String sql) {
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      int count = 0;
      while (rs.next()) count++;
      return count;
    } catch (Exception e) {
      return -1;
    }
  }

  private void exec(Connection conn, String sql) {
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(sql);
    } catch (Exception ignored) {
    }
  }
}
