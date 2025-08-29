package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.common.util.InsertStatementParser.InsertInfo;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class InsertStatementParserTest {

  @Test
  void testParseBasicInsert() {
    String sql = "INSERT INTO users (id, name, email) VALUES (?, ?, ?)";
    InsertInfo info = InsertStatementParser.parseInsert(sql);

    assertNotNull(info);
    assertEquals("users", info.getTableName());
    assertEquals(Arrays.asList("id", "name", "email"), info.getColumns());
    assertEquals(sql, info.getOriginalSql());
  }

  @Test
  void testParseInsertWithWhitespace() {
    String sql = "   INSERT   INTO   users   (  id  ,  name  ,  email  )   VALUES   ( ?, ?, ? )";
    InsertInfo info = InsertStatementParser.parseInsert(sql);

    assertNotNull(info);
    assertEquals("users", info.getTableName());
    assertEquals(Arrays.asList("id", "name", "email"), info.getColumns());
  }

  @Test
  void testParseInsertWithBackticks() {
    String sql = "INSERT INTO `my_table` (`id`, `user_name`, `email_address`) VALUES (?, ?, ?)";
    InsertInfo info = InsertStatementParser.parseInsert(sql);

    assertNotNull(info);
    assertEquals("`my_table`", info.getTableName());
    assertEquals(Arrays.asList("id", "user_name", "email_address"), info.getColumns());
  }

  @Test
  void testParseInsertWithSchemaPrefix() {
    String sql = "INSERT INTO schema.users (id, name) VALUES (?, ?)";
    InsertInfo info = InsertStatementParser.parseInsert(sql);

    assertNotNull(info);
    assertEquals("schema.users", info.getTableName());
    assertEquals(Arrays.asList("id", "name"), info.getColumns());
  }

  @Test
  void testParseInsertCaseInsensitive() {
    String sql = "insert into Users (ID, Name) values (?, ?)";
    InsertInfo info = InsertStatementParser.parseInsert(sql);

    assertNotNull(info);
    assertEquals("Users", info.getTableName());
    assertEquals(Arrays.asList("ID", "Name"), info.getColumns());
  }

  @Test
  void testParseInvalidSql() {
    assertNull(InsertStatementParser.parseInsert("SELECT * FROM users"));
    assertNull(InsertStatementParser.parseInsert("UPDATE users SET name = ?"));
    assertNull(InsertStatementParser.parseInsert("DELETE FROM users"));
    assertNull(InsertStatementParser.parseInsert(null));
    assertNull(InsertStatementParser.parseInsert(""));
    assertNull(InsertStatementParser.parseInsert("   "));
  }

  @Test
  void testParseInsertWithoutValues() {
    String sql = "INSERT INTO users (id, name)";
    InsertInfo info = InsertStatementParser.parseInsert(sql);
    assertNull(info);
  }

  @Test
  void testParseInsertWithoutColumns() {
    String sql = "INSERT INTO users VALUES (?, ?)";
    InsertInfo info = InsertStatementParser.parseInsert(sql);
    assertNull(info);
  }

  @Test
  void testIsParametrizedInsert() {
    assertTrue(
        InsertStatementParser.isParametrizedInsert("INSERT INTO users (id, name) VALUES (?, ?)"));
    assertFalse(
        InsertStatementParser.isParametrizedInsert(
            "INSERT INTO users (id, name) VALUES (1, 'John')"));
    assertFalse(InsertStatementParser.isParametrizedInsert("SELECT * FROM users"));
    assertFalse(InsertStatementParser.isParametrizedInsert(null));
  }

  @Test
  void testInsertInfoCompatibility() {
    InsertInfo info1 =
        InsertStatementParser.parseInsert("INSERT INTO users (id, name) VALUES (?, ?)");
    InsertInfo info2 =
        InsertStatementParser.parseInsert("INSERT INTO users (id, name) VALUES (?, ?)");
    InsertInfo info3 =
        InsertStatementParser.parseInsert("INSERT INTO users (id, email) VALUES (?, ?)");
    InsertInfo info4 =
        InsertStatementParser.parseInsert("INSERT INTO orders (id, name) VALUES (?, ?)");

    assertNotNull(info1);
    assertNotNull(info2);
    assertNotNull(info3);
    assertNotNull(info4);

    assertTrue(info1.isCompatibleWith(info2));
    assertFalse(info1.isCompatibleWith(info3)); // Different columns
    assertFalse(info1.isCompatibleWith(info4)); // Different table
  }

  @Test
  void testGenerateMultiRowInsert() throws Exception {
    InsertInfo info =
        InsertStatementParser.parseInsert("INSERT INTO users (id, name, email) VALUES (?, ?, ?)");
    assertNotNull(info);

    String multiRowSql = InsertStatementParser.generateMultiRowInsert(info, 3);
    String expected = "INSERT INTO users (id, name, email) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)";
    assertEquals(expected, multiRowSql);
  }

  @Test
  void testGenerateMultiRowInsertSingleRow() throws Exception {
    InsertInfo info =
        InsertStatementParser.parseInsert("INSERT INTO users (id, name) VALUES (?, ?)");
    assertNotNull(info);

    String multiRowSql = InsertStatementParser.generateMultiRowInsert(info, 1);
    String expected = "INSERT INTO users (id, name) VALUES (?, ?)";
    assertEquals(expected, multiRowSql);
  }

  @Test
  void testGenerateMultiRowInsertInvalidInput() {
    InsertInfo info =
        InsertStatementParser.parseInsert("INSERT INTO users (id, name) VALUES (?, ?)");
    assertNotNull(info);

    // Test that exceptions are thrown for invalid inputs
    assertThrows(Exception.class, () -> InsertStatementParser.generateMultiRowInsert(null, 3));
    assertThrows(Exception.class, () -> InsertStatementParser.generateMultiRowInsert(info, 0));
    assertThrows(Exception.class, () -> InsertStatementParser.generateMultiRowInsert(info, -1));
  }

  @Test
  void testInsertInfoEqualsAndHashCode() {
    InsertInfo info1 =
        new InsertInfo(
            "users", List.of("id", "name"), "INSERT INTO users (id, name) VALUES (?, ?)");
    InsertInfo info2 =
        new InsertInfo(
            "users", List.of("id", "name"), "INSERT INTO users (id, name) VALUES (?, ?)");
    InsertInfo info3 =
        new InsertInfo(
            "users", List.of("id", "email"), "INSERT INTO users (id, email) VALUES (?, ?)");

    assertEquals(info1, info2);
    assertNotEquals(info1, info3);
    assertEquals(info1.hashCode(), info2.hashCode());
    assertNotEquals(info1.hashCode(), info3.hashCode());
  }

  @Test
  void testGetColumnCount() {
    InsertInfo info2Cols =
        InsertStatementParser.parseInsert("INSERT INTO users (id, name) VALUES (?, ?)");
    assertNotNull(info2Cols);
    assertEquals(2, info2Cols.getColumnCount());

    InsertInfo info5Cols =
        InsertStatementParser.parseInsert(
            "INSERT INTO products (id, name, price, category, description) VALUES (?, ?, ?, ?, ?)");
    assertNotNull(info5Cols);
    assertEquals(5, info5Cols.getColumnCount());

    InsertInfo info1Col = InsertStatementParser.parseInsert("INSERT INTO simple (id) VALUES (?)");
    assertNotNull(info1Col);
    assertEquals(1, info1Col.getColumnCount());
  }

  @Test
  void testParameterLimitCalculations() {
    // Test parameter limit calculations that would be used in chunking logic

    // 5 columns: 256/5 = 51 rows per chunk
    InsertInfo info5Cols =
        InsertStatementParser.parseInsert(
            "INSERT INTO products (id, name, price, category, description) VALUES (?, ?, ?, ?, ?)");
    assertNotNull(info5Cols);
    int maxRowsFor5Cols = 256 / info5Cols.getColumnCount();
    assertEquals(51, maxRowsFor5Cols);

    // 2 columns: 256/2 = 128 rows per chunk
    InsertInfo info2Cols =
        InsertStatementParser.parseInsert("INSERT INTO users (id, name) VALUES (?, ?)");
    assertNotNull(info2Cols);
    int maxRowsFor2Cols = 256 / info2Cols.getColumnCount();
    assertEquals(128, maxRowsFor2Cols);

    // 10 columns: 256/10 = 25 rows per chunk
    InsertInfo info10Cols =
        InsertStatementParser.parseInsert(
            "INSERT INTO wide_table (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    assertNotNull(info10Cols);
    int maxRowsFor10Cols = 256 / info10Cols.getColumnCount();
    assertEquals(25, maxRowsFor10Cols);

    // Edge case: 300 columns would result in 0 rows per chunk, should be handled as 1
    InsertInfo info300Cols = InsertStatementParser.parseInsert(generateLargeInsert(300));
    assertNotNull(info300Cols);
    assertEquals(300, info300Cols.getColumnCount());
    int maxRowsFor300Cols = 256 / info300Cols.getColumnCount();
    assertEquals(0, maxRowsFor300Cols); // This would need to be handled as 1 in the actual code
  }

  @Test
  void testChunkingScenarios() {
    // Test realistic chunking scenarios

    // Scenario 1: Large batch with 5 columns, 10000 rows
    InsertInfo info5Cols =
        InsertStatementParser.parseInsert(
            "INSERT INTO products (id, name, price, category, description) VALUES (?, ?, ?, ?, ?)");
    assertNotNull(info5Cols);
    assertEquals(5, info5Cols.getColumnCount());

    int totalRows = 10000;
    int maxRowsPerChunk = 256 / info5Cols.getColumnCount(); // 51 rows per chunk
    int expectedChunks = (int) Math.ceil((double) totalRows / maxRowsPerChunk); // 197 chunks
    assertEquals(51, maxRowsPerChunk);
    assertEquals(197, expectedChunks);

    // Scenario 2: Batch that would exceed parameter limit in one go
    InsertInfo info2Cols =
        InsertStatementParser.parseInsert("INSERT INTO users (id, name) VALUES (?, ?)");
    assertNotNull(info2Cols);
    assertEquals(2, info2Cols.getColumnCount());

    int batchSize = 200; // Would be 400 parameters (200 * 2 columns), exceeding 256 limit
    int maxRowsFor2Cols = 256 / info2Cols.getColumnCount(); // 128 rows per chunk
    int neededChunks = (int) Math.ceil((double) batchSize / maxRowsFor2Cols); // 2 chunks
    assertEquals(128, maxRowsFor2Cols);
    assertEquals(2, neededChunks);
  }

  private String generateLargeInsert(int columnCount) {
    StringBuilder columns = new StringBuilder();
    StringBuilder values = new StringBuilder();

    for (int i = 1; i <= columnCount; i++) {
      if (i > 1) {
        columns.append(", ");
        values.append(", ");
      }
      columns.append("col").append(i);
      values.append("?");
    }

    return "INSERT INTO large_table (" + columns + ") VALUES (" + values + ")";
  }
}
