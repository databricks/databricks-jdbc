package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static com.databricks.jdbc.api.impl.DatabricksPreparedStatementTest.getSqlParam;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.databricks.jdbc.api.impl.ImmutableSqlParameter;
import com.databricks.jdbc.exception.DatabricksValidationException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SQLInterpolatorTest {

  @Test
  public void testInterpolateSQLWithStrings() throws DatabricksValidationException {
    String sql = "SELECT * FROM users WHERE name = ? AND city = ?";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, "Alice", DatabricksTypeUtil.STRING));
    params.put(2, getSqlParam(2, "Wonderland", DatabricksTypeUtil.STRING));
    String expected = "SELECT * FROM users WHERE name = 'Alice' AND city = 'Wonderland'";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testInterpolateSQLWithMixedTypes() throws DatabricksValidationException {
    String sql = "INSERT INTO sales (id, amount, active) VALUES (?, ?, ?)";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, 101, DatabricksTypeUtil.INT));
    params.put(2, getSqlParam(2, 19.95, DatabricksTypeUtil.FLOAT));
    params.put(3, getSqlParam(3, true, DatabricksTypeUtil.BOOLEAN));
    String expected = "INSERT INTO sales (id, amount, active) VALUES (101, 19.95, true)";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testInterpolateSQLWithNullValues() throws DatabricksValidationException {
    String sql = "UPDATE products SET price = ? WHERE id = ?";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, null, DatabricksTypeUtil.NULL));
    params.put(2, getSqlParam(2, 200, DatabricksTypeUtil.INT));
    String expected = "UPDATE products SET price = NULL WHERE id = 200";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testParameterMismatch() {
    String sql = "DELETE FROM log WHERE date = ?";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>(); // no parameters added
    assertThrows(
        DatabricksValidationException.class,
        () -> {
          SQLInterpolator.interpolateSQL(sql, params);
        });
  }

  @Test
  public void testExtraParameters() {
    String sql = "SELECT * FROM clients WHERE client_id = ?";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, 300, DatabricksTypeUtil.INT));
    params.put(2, getSqlParam(2, TEST_STRING, DatabricksTypeUtil.STRING)); // extra parameter
    assertThrows(
        DatabricksValidationException.class,
        () -> {
          SQLInterpolator.interpolateSQL(sql, params);
        });
  }

  @Test
  public void testEscapedValues() throws DatabricksValidationException {
    String sql = "UPDATE products SET price = ? WHERE id = ?";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, "O'Reilly", DatabricksTypeUtil.STRING));
    params.put(2, getSqlParam(2, 200, DatabricksTypeUtil.INT));
    String expected = "UPDATE products SET price = 'O''Reilly' WHERE id = 200";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testBinaryType() throws DatabricksValidationException {
    String sql = "INSERT INTO sales (id, data) VALUES (?, ?)";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, 101, DatabricksTypeUtil.INT));
    params.put(2, getSqlParam(2, "X'0102030405'", DatabricksTypeUtil.BINARY));
    String expected = "INSERT INTO sales (id, data) VALUES (101, X'0102030405')";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testTimestampType() throws DatabricksValidationException {
    String sql = "INSERT INTO events (id, created_at) VALUES (?, ?)";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    Timestamp timestamp = Timestamp.valueOf("2024-01-01 12:30:45.123");
    params.put(1, getSqlParam(1, 101, DatabricksTypeUtil.INT));
    params.put(2, getSqlParam(2, timestamp, DatabricksTypeUtil.TIMESTAMP));
    String expected = "INSERT INTO events (id, created_at) VALUES (101, '2024-01-01 12:30:45.123')";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testDateType() throws DatabricksValidationException {
    String sql = "INSERT INTO events (id, event_date) VALUES (?, ?)";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    Date date = Date.valueOf("2024-01-01");
    params.put(1, getSqlParam(1, 101, DatabricksTypeUtil.INT));
    params.put(2, getSqlParam(2, date, DatabricksTypeUtil.DATE));
    String expected = "INSERT INTO events (id, event_date) VALUES (101, '2024-01-01')";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testStringWithNewline() throws DatabricksValidationException {
    String sql = "INSERT INTO events (id, text) VALUES (?, ?)";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, 101, DatabricksTypeUtil.INT));
    params.put(2, getSqlParam(2, "foo\nbar\tbazz", DatabricksTypeUtil.STRING));
    String expected = "INSERT INTO events (id, text) VALUES (101, 'foo\\nbar\\tbazz')";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testQuestionMarkInLineCommentNotTreatedAsPlaceholder()
      throws DatabricksValidationException {
    String sql = "-- does this work?\nselect * from mytable where id = ?";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, 42, DatabricksTypeUtil.INT));
    String expected = "-- does this work?\nselect * from mytable where id = 42";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testQuestionMarkInBlockCommentNotTreatedAsPlaceholder()
      throws DatabricksValidationException {
    String sql = "select /* maybe? or ? */ * from t where id = ?";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, 1, DatabricksTypeUtil.INT));
    String expected = "select /* maybe? or ? */ * from t where id = 1";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testQuestionMarkInStringLiteralNotTreatedAsPlaceholder()
      throws DatabricksValidationException {
    String sql = "select 'hello?', * from mytable where id = ?";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, 5, DatabricksTypeUtil.INT));
    String expected = "select 'hello?', * from mytable where id = 5";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testQuestionMarkInQuotedIdentifierNotTreatedAsPlaceholder()
      throws DatabricksValidationException {
    String sql = "select `col?` from t where id = ? and name = \"who?\"";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, 7, DatabricksTypeUtil.INT));
    String expected = "select `col?` from t where id = 7 and name = \"who?\"";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testCombinedCommentsAndLiteralsWithQuestionMarks()
      throws DatabricksValidationException {
    String sql =
        "-- ?\n/* ? */ select 'a?' as x, ? as y, \"b?\" as z, `c?` as w from t where id = ?";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, "alice", DatabricksTypeUtil.STRING));
    params.put(2, getSqlParam(2, 99, DatabricksTypeUtil.INT));
    String expected =
        "-- ?\n/* ? */ select 'a?' as x, 'alice' as y, \"b?\" as z, `c?` as w from t where id = 99";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testInterpolateAdjacentPlaceholders() throws DatabricksValidationException {
    String sql = "select ?,?,? from t";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, 1, DatabricksTypeUtil.INT));
    params.put(2, getSqlParam(2, 2, DatabricksTypeUtil.INT));
    params.put(3, getSqlParam(3, 3, DatabricksTypeUtil.INT));
    assertEquals("select 1,2,3 from t", SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testInterpolatePlaceholderAtStartAndEnd() throws DatabricksValidationException {
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, 7, DatabricksTypeUtil.INT));
    assertEquals("7 ", SQLInterpolator.interpolateSQL("? ", params));
    assertEquals(" 7", SQLInterpolator.interpolateSQL(" ?", params));
    assertEquals("7", SQLInterpolator.interpolateSQL("?", params));
  }

  @Test
  public void testEscapeInputs() {
    // Simple apostrophe doubling
    assertEquals("'foo''bar'", SQLInterpolator.escapeInputs("foo'bar"));
    // Escaping newlines
    assertEquals("'line1\\nline2'", SQLInterpolator.escapeInputs("line1\nline2"));
    // Escaping backslashes
    assertEquals("'back\\\\slash'", SQLInterpolator.escapeInputs("back\\slash"));
    // Emoji properly escaped as \U0001F9F1
    assertEquals("'brick\\U0001F9F1test'", SQLInterpolator.escapeInputs("brick🧱test"));
  }

  private static Stream<Arguments> providePlaceholderQuotingTestCases() {
    return Stream.of(
        // Basic placeholder quoting
        Arguments.of(
            "SELECT * FROM table WHERE id = ?",
            "SELECT * FROM table WHERE id = '?'",
            "Basic placeholder quoting"),

        // Multiple placeholders
        Arguments.of(
            "SELECT * FROM table WHERE id = ? AND name = ?",
            "SELECT * FROM table WHERE id = '?' AND name = '?'",
            "Multiple placeholders"),

        // Already quoted placeholders
        Arguments.of(
            "SELECT * FROM table WHERE id = '?' AND name = ?",
            "SELECT * FROM table WHERE id = '?' AND name = '?'",
            "Already quoted placeholders"),

        // Mixed quoted and unquoted placeholders
        Arguments.of(
            "SELECT * FROM table WHERE id = '?' AND name = ? AND age = '?'",
            "SELECT * FROM table WHERE id = '?' AND name = '?' AND age = '?'",
            "Mixed quoted and unquoted placeholders"),

        // Null input
        Arguments.of(null, null, "Null input"),

        // Empty input
        Arguments.of("", "", "Empty input"),

        // No placeholders
        Arguments.of("SELECT * FROM table", "SELECT * FROM table", "No placeholders"),

        // Complex query with multiple conditions
        Arguments.of(
            "SELECT * FROM table WHERE id = ? AND (name = ? OR age = ?) AND status = ?",
            "SELECT * FROM table WHERE id = '?' AND (name = '?' OR age = '?') AND status = '?'",
            "Complex query with multiple conditions"),

        // ? inside line comment must not be quoted
        Arguments.of(
            "-- is this a ?\nSELECT ? FROM t",
            "-- is this a ?\nSELECT '?' FROM t",
            "Question mark inside line comment is preserved"),

        // ? inside block comment must not be quoted
        Arguments.of(
            "SELECT /* ? */ ? FROM t",
            "SELECT /* ? */ '?' FROM t",
            "Question mark inside block comment is preserved"),

        // ? inside double-quoted identifier must not be quoted
        Arguments.of(
            "SELECT \"col?\" FROM t WHERE id = ?",
            "SELECT \"col?\" FROM t WHERE id = '?'",
            "Question mark inside double-quoted identifier is preserved"),

        // ? inside backtick identifier must not be quoted
        Arguments.of(
            "SELECT `col?` FROM t WHERE id = ?",
            "SELECT `col?` FROM t WHERE id = '?'",
            "Question mark inside backtick identifier is preserved"),

        // Adjacent placeholders — both must be quoted independently
        Arguments.of(
            "SELECT ?,?,? FROM t",
            "SELECT '?','?','?' FROM t",
            "Adjacent placeholders are each quoted"));
  }

  @ParameterizedTest(name = "{2}")
  @MethodSource("providePlaceholderQuotingTestCases")
  public void testSurroundPlaceholdersWithQuotes(String input, String expected, String testName) {
    assertEquals(expected, SQLInterpolator.surroundPlaceholdersWithQuotes(input), testName);
  }
}
