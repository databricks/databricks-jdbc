package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.error.CapturedOutcome;
import com.databricks.jdbc.comparator.error.Captures;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Negative type-conversion cases — both endpoints return a row successfully, but a {@code
 * ResultSet.getX()} call converts a cell to an incompatible target and may fail. Compares how each
 * endpoint surfaces the conversion (a value, or a thrown error compared by class/SQLState/code).
 *
 * <p>Read-only: only SELECTs and getters, safe on the shared connections. The query selects a
 * single known row so both sides read identical cell values before the conversion is attempted.
 */
public class NegativeTypeConversionProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";

  /** Reads column 1 of the first row via a getter that may fail to convert. */
  @FunctionalInterface
  private interface Getter {
    Object get(ResultSet rs) throws Exception;
  }

  private static final class Case {
    final String description;
    final String query;
    final Getter getter;

    Case(String description, String query, Getter getter) {
      this.description = description;
      this.query = query;
      this.getter = getter;
    }
  }

  private static final List<Case> CASES =
      Arrays.asList(
          new Case(
              "getInt() on an out-of-int-range BIGINT",
              "SELECT bigint_column FROM " + TABLE + " WHERE bigint_column > 3000000000 LIMIT 1",
              rs -> rs.getInt(1)),
          new Case(
              "getByte() on an out-of-byte-range INTEGER",
              "SELECT integer_column FROM " + TABLE + " WHERE integer_column > 1000 LIMIT 1",
              rs -> rs.getByte(1)),
          new Case(
              "getInt() on a non-numeric VARCHAR",
              "SELECT varchar_column FROM "
                  + TABLE
                  + " WHERE varchar_column IS NOT NULL AND varchar_column NOT RLIKE '^[0-9]+$'"
                  + " LIMIT 1",
              rs -> rs.getInt(1)),
          new Case(
              "getObject(column, Integer.class) on a VARCHAR",
              "SELECT varchar_column FROM " + TABLE + " WHERE varchar_column IS NOT NULL LIMIT 1",
              rs -> rs.getObject(1, Integer.class)),
          new Case(
              "getInt() on an ARRAY column",
              "SELECT array_column FROM " + TABLE + " WHERE array_column IS NOT NULL LIMIT 1",
              rs -> rs.getInt(1)),
          new Case(
              "getBigDecimal() on a STRUCT column",
              "SELECT struct_column FROM " + TABLE + " WHERE struct_column IS NOT NULL LIMIT 1",
              rs -> rs.getBigDecimal(1)),
          // Cross-type complex getters: the getter targets the WRONG complex type, so it hits an
          // error path in both configs — the disabled-support guard when complex types are off, or
          // a ClassCastException when they are on. (Type-matched getters would succeed under
          // complex-enabled, giving a vacuous NOT_APPLICABLE pass with no value comparison.)
          new Case(
              "getMap() on an ARRAY column (wrong complex type)",
              "SELECT array_column FROM " + TABLE + " WHERE array_column IS NOT NULL LIMIT 1",
              rs -> rs.unwrap(IDatabricksResultSet.class).getMap(1)),
          new Case(
              "getArray() on a MAP column (wrong complex type)",
              "SELECT map_column FROM " + TABLE + " WHERE map_column IS NOT NULL LIMIT 1",
              rs -> rs.getArray(1)),
          new Case(
              "getStruct() on an ARRAY column (wrong complex type)",
              "SELECT array_column FROM " + TABLE + " WHERE array_column IS NOT NULL LIMIT 1",
              rs -> rs.unwrap(IDatabricksResultSet.class).getStruct(1)));

  @Override
  public List<TestCase> getTestCases() {
    return CASES.stream()
        .map(c -> new TestCase(c.description, c.description))
        .collect(Collectors.toList());
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    Case c =
        CASES.stream()
            .filter(x -> x.description.equals(testCase.getIdentifier()))
            .findFirst()
            .orElseThrow(
                () -> new IllegalArgumentException("Unknown case: " + testCase.getIdentifier()));

    try (Statement s1 = conn1.createStatement();
        Statement s2 = conn2.createStatement();
        ResultSet rs1 = s1.executeQuery(c.query);
        ResultSet rs2 = s2.executeQuery(c.query)) {
      // Both queries must return the same shape; if a side has no row, the getter can't run — that
      // is a harness/data problem, so let it propagate (fail loudly) rather than compare noise.
      boolean has1 = rs1.next();
      boolean has2 = rs2.next();
      if (!has1 || !has2) {
        throw new IllegalStateException(
            "Type-conversion case '"
                + c.description
                + "' expected a row on both sides but got "
                + "left="
                + has1
                + " right="
                + has2);
      }
      CapturedOutcome left = Captures.capture(() -> c.getter.get(rs1));
      CapturedOutcome right = Captures.capture(() -> c.getter.get(rs2));
      return Captures.compareCall(
          label, c.description, testCase.getArgs(), left, right, v -> "value " + v);
    }
  }
}
