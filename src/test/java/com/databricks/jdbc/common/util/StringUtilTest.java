package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StringUtilTest {

  @Test
  public void testDateEscapeSequence() {
    String sqlWithDate = "SELECT * FROM table WHERE date_column = {d '2023-01-01'}";
    String expected = "SELECT * FROM table WHERE date_column = DATE '2023-01-01'";
    assertEquals(expected, StringUtil.convertJdbcEscapeSequences(sqlWithDate));
  }

  @Test
  public void testTimeEscapeSequence() {
    String sqlWithTime = "SELECT * FROM table WHERE time_column = {t '23:59:59'}";
    String expected = "SELECT * FROM table WHERE time_column = TIME '23:59:59'";
    assertEquals(expected, StringUtil.convertJdbcEscapeSequences(sqlWithTime));
  }

  @Test
  public void testTimestampEscapeSequence() {
    String sqlWithTimestamp =
        "SELECT * FROM table WHERE timestamp_column = {ts '2023-01-01 23:59:59.123'}";
    String expected =
        "SELECT * FROM table WHERE timestamp_column = TIMESTAMP '2023-01-01 23:59:59.123'";
    assertEquals(expected, StringUtil.convertJdbcEscapeSequences(sqlWithTimestamp));
  }

  @Test
  public void testFunctionEscapeSequence() {
    String sqlWithFunction = "SELECT {fn UCASE('name')} FROM table";
    String expected = "SELECT UCASE('name') FROM table";
    assertEquals(expected, StringUtil.convertJdbcEscapeSequences(sqlWithFunction));
  }

  @Test
  public void testNoEscapeSequence() {
    String sqlWithoutEscape = "SELECT * FROM table WHERE column = 'value'";
    assertEquals(sqlWithoutEscape, StringUtil.convertJdbcEscapeSequences(sqlWithoutEscape));
  }

  @Test
  public void testOuterJoinEscapeSequence() {
    String sqlWithOuterJoin = "{oj table1 LEFT OUTER JOIN table2 ON table1.id = table2.id}";
    String expected = "table1 LEFT OUTER JOIN table2 ON table1.id = table2.id";
    assertEquals(expected, StringUtil.convertJdbcEscapeSequences(sqlWithOuterJoin));
  }

  @Test
  public void testNestedOuterJoinEscapeSequence() {
    String sqlWithNestedOuterJoin =
        "{oj table1 LEFT OUTER JOIN table2 ON table1.id = table2.id {nested}}";
    String expected = "table1 LEFT OUTER JOIN table2 ON table1.id = table2.id {nested}";
    assertEquals(expected, StringUtil.convertJdbcEscapeSequences(sqlWithNestedOuterJoin));
  }

  @Test
  public void testStoredProcedureCallEscapeSequence() {
    String sqlWithCall = "{call myProcedure(?, ?)}";
    String expected = "CALL myProcedure(?, ?)";
    assertEquals(expected, StringUtil.convertJdbcEscapeSequences(sqlWithCall));
  }

  @Test
  public void testEscapeStringLiteral() {
    String sqlValue = "'1';select * from other-table";
    String expected = "''1'';select * from other-table";
    assertEquals(expected, StringUtil.escapeStringLiteral(sqlValue));
  }

  @Test
  public void testRemoveEmptyEscapeClause() {
    String sql = "SELECT * FROM table WHERE name LIKE 'pattern%' ESCAPE ''";
    String expected = "SELECT * FROM table WHERE name LIKE 'pattern%'";
    assertEquals(expected, StringUtil.removeRedundantEscapeClause(sql));
  }

  @Test
  public void testPreserveCustomEscapeClause() {
    String sql = "SELECT * FROM table WHERE name LIKE 'pattern#%' ESCAPE '#'";
    assertEquals(sql, StringUtil.removeRedundantEscapeClause(sql));
  }

  @Test
  public void testParseIntegerSet() {
    // Valid input
    Set<Integer> result = StringUtil.parseIntegerSet("500,503,504");
    assertEquals(3, result.size());
    assertTrue(result.contains(500));
    assertTrue(result.contains(503));
    assertTrue(result.contains(504));

    // Empty or whitespace
    assertTrue(StringUtil.parseIntegerSet("").isEmpty());
    assertTrue(StringUtil.parseIntegerSet("   ").isEmpty());
  }
}
