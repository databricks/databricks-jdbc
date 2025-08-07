package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static java.sql.ParameterMetaData.parameterModeIn;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DatabricksParameterMetaDataTest {
  private DatabricksParameterMetaData metaData;

  @BeforeEach
  public void setUp() {
    metaData = new DatabricksParameterMetaData();
    metaData.put(
        1,
        ImmutableSqlParameter.builder()
            .type(ColumnInfoTypeName.STRING)
            .cardinal(1)
            .value(TEST_STRING)
            .build());
    metaData.put(
        2,
        ImmutableSqlParameter.builder()
            .type(ColumnInfoTypeName.INT)
            .cardinal(1)
            .value(123)
            .build());
  }

  @Test
  public void testInitialization() {
    DatabricksParameterMetaData newMetadata = new DatabricksParameterMetaData();
    assertTrue(newMetadata.getParameterBindings().isEmpty());
    assertEquals(2, metaData.getParameterBindings().size());
  }

  @Test
  public void testClear() {
    metaData.clear();
    assertTrue(metaData.getParameterBindings().isEmpty());
  }

  @Test
  public void testGetParameterMode() throws SQLException {
    assertEquals(parameterModeIn, metaData.getParameterMode(1));
  }

  @Test
  public void testGetParameterClassName() throws SQLException {
    assertEquals("java.lang.String", metaData.getParameterClassName(1));
    assertEquals("java.lang.Integer", metaData.getParameterClassName(2));
  }

  @Test
  public void testGetParameterTypeName() throws SQLException {
    assertEquals("STRING", metaData.getParameterTypeName(1));
    assertEquals("INT", metaData.getParameterTypeName(2));
  }

  @Test
  public void testGetParameterType() throws SQLException {
    assertEquals(Types.VARCHAR, metaData.getParameterType(1));
    assertEquals(metaData.getParameterType(2), Types.INTEGER);
  }

  @Test
  public void testConstructorWithNullSql() throws SQLException {
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(null);
    assertEquals(0, metadata.getParameterCount());
  }

  @Test
  public void testConstructorWithEmptySql() throws SQLException {
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData("");
    assertEquals(0, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountWithSimpleQuery() throws SQLException {
    String sql = "SELECT * FROM table WHERE id = ? AND name = ?";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(2, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountWithNoParameters() throws SQLException {
    String sql = "SELECT * FROM table WHERE id = 1 AND name = 'test'";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(0, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountIgnoresParametersInSingleQuotes() throws SQLException {
    String sql = "SELECT * FROM table WHERE name = 'has ? in quotes' AND id = ?";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(1, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountIgnoresParametersInDoubleQuotes() throws SQLException {
    String sql = "SELECT * FROM table WHERE name = \"has ? in quotes\" AND id = ?";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(1, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountWithEscapedSingleQuotes() throws SQLException {
    String sql =
        "SELECT * FROM table WHERE name = 'has '' escaped quote and ? parameter' AND id = ?";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(1, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountWithEscapedDoubleQuotes() throws SQLException {
    String sql =
        "SELECT * FROM table WHERE name = \"has \"\" escaped quote and ? parameter\" AND id = ?";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(1, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountIgnoresParametersInLineComments() throws SQLException {
    String sql = "SELECT * FROM table WHERE id = ? -- comment with ? parameter\nAND name = ?";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(2, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountIgnoresParametersInBlockComments() throws SQLException {
    String sql =
        "SELECT * FROM table WHERE id = ? /* block comment with ? parameter */ AND name = ?";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(2, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountWithMultilineBlockComment() throws SQLException {
    String sql =
        "SELECT * FROM table WHERE id = ? /* multi-line\n comment with ? parameter\n */ AND name = ?";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(2, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountWithComplexQuery() throws SQLException {
    String sql =
        "SELECT t1.id, t2.name "
            + "FROM table1 t1 "
            + "JOIN table2 t2 ON t1.id = t2.id "
            + "WHERE t1.status = ? "
            + "AND t2.created_date > ? "
            + "AND t1.description NOT LIKE '%test?value%' -- comment with ?\n"
            + "ORDER BY t1.id LIMIT ?";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(3, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountWithMixedQuotesAndComments() throws SQLException {
    String sql =
        "SELECT * FROM table WHERE col1 = 'value with ? mark' AND col2 = ? /* comment ? */ AND col3 = \"another ? value\" AND col4 = ?";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(2, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountAfterBindingParameters() throws SQLException {
    String sql = "SELECT * FROM table WHERE id = ? AND name = ? AND status = ?";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);

    // Initially should return count based on SQL parsing
    assertEquals(3, metadata.getParameterCount());

    // Add some bindings - count should still be based on SQL, not bindings
    metadata.put(
        1,
        ImmutableSqlParameter.builder()
            .type(ColumnInfoTypeName.INT)
            .cardinal(1)
            .value(123)
            .build());

    assertEquals(3, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountWithInsertStatement() throws SQLException {
    String sql = "INSERT INTO table (col1, col2, col3) VALUES (?, ?, ?)";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(3, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountWithUpdateStatement() throws SQLException {
    String sql = "UPDATE table SET col1 = ?, col2 = ? WHERE id = ? AND status = ?";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(4, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountWithNestedQuotes() throws SQLException {
    String sql =
        "SELECT * FROM table WHERE col1 = 'outer ''inner ? not counted'' outer' AND col2 = ?";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(1, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountWithCarriageReturnInComment() throws SQLException {
    String sql = "SELECT * FROM table WHERE id = ? -- comment with ? \r\nAND name = ?";
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);
    assertEquals(2, metadata.getParameterCount());
  }

  @Test
  public void testParameterCountExceedsBindingsThrowsException() {
    String sql = "SELECT * FROM table WHERE id = ? AND name = ?"; // 2 parameters
    DatabricksParameterMetaData metadata = new DatabricksParameterMetaData(sql);

    // Add 3 parameter bindings (more than the 2 parameters in SQL)
    metadata.put(
        1,
        ImmutableSqlParameter.builder().type(ColumnInfoTypeName.INT).cardinal(1).value(1).build());
    metadata.put(
        2,
        ImmutableSqlParameter.builder()
            .type(ColumnInfoTypeName.STRING)
            .cardinal(2)
            .value("test")
            .build());
    metadata.put(
        3,
        ImmutableSqlParameter.builder()
            .type(ColumnInfoTypeName.STRING)
            .cardinal(3)
            .value("extra")
            .build());

    // getParameterCount should throw SQLException due to too many bindings
    SQLException exception = assertThrows(SQLException.class, metadata::getParameterCount);
    assertTrue(
        exception
            .getMessage()
            .contains("Number of parameter bindings (3) exceeds parameter count (2)"));
  }
}
