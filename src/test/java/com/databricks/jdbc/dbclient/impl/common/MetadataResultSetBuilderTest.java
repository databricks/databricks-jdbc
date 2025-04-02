package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.common.MetadataResultConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.model.core.ResultColumn;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class MetadataResultSetBuilderTest {

  @BeforeEach
  void setUp() {
    DatabricksThreadContextHolder.setConnectionContext(
        Mockito.mock(IDatabricksConnectionContext.class));
    when(DatabricksThreadContextHolder.getConnectionContext().getDefaultStringColumnLength())
        .thenReturn(255);
  }

  @AfterEach
  void tearDown() {
    DatabricksThreadContextHolder.clearAllContext();
  }

  @Test
  void testGetCode() {
    assert MetadataResultSetBuilder.getCode("STRING") == 12;
    assert MetadataResultSetBuilder.getCode("INT") == 4;
    assert MetadataResultSetBuilder.getCode("DOUBLE") == 8;
    assert MetadataResultSetBuilder.getCode("FLOAT") == 6;
    assert MetadataResultSetBuilder.getCode("BOOLEAN") == 16;
    assert MetadataResultSetBuilder.getCode("DATE") == 91;
    assert MetadataResultSetBuilder.getCode("TIMESTAMP_NTZ") == 93;
    assert MetadataResultSetBuilder.getCode("TIMESTAMP") == 93;
    assert MetadataResultSetBuilder.getCode("DECIMAL") == 3;
    assert MetadataResultSetBuilder.getCode("BINARY") == -2;
    assert MetadataResultSetBuilder.getCode("ARRAY") == 2003;
    assert MetadataResultSetBuilder.getCode("MAP") == 2002;
    assert MetadataResultSetBuilder.getCode("STRUCT") == 2002;
    assert MetadataResultSetBuilder.getCode("UNIONTYPE") == 2002;
    assert MetadataResultSetBuilder.getCode("BYTE") == -6;
    assert MetadataResultSetBuilder.getCode("SHORT") == 5;
    assert MetadataResultSetBuilder.getCode("LONG") == -5;
    assert MetadataResultSetBuilder.getCode("NULL") == 0;
    assert MetadataResultSetBuilder.getCode("VOID") == 0;
    assert MetadataResultSetBuilder.getCode("CHAR") == 1;
    assert MetadataResultSetBuilder.getCode("VARCHAR") == 12;
    assert MetadataResultSetBuilder.getCode("CHARACTER") == 1;
    assert MetadataResultSetBuilder.getCode("BIGINT") == -5;
    assert MetadataResultSetBuilder.getCode("TINYINT") == -6;
    assert MetadataResultSetBuilder.getCode("SMALLINT") == 5;
    assert MetadataResultSetBuilder.getCode("INTEGER") == 4;
    assert MetadataResultSetBuilder.getCode("VARIANT") == 1111;
  }

  private static Stream<Arguments> provideSqlTypesAndExpectedSizes() {
    return Stream.of(
        Arguments.of(Types.TIME, 6),
        Arguments.of(Types.DATE, 6),
        Arguments.of(Types.TIMESTAMP, 16),
        Arguments.of(Types.NUMERIC, 40),
        Arguments.of(Types.DECIMAL, 40),
        Arguments.of(Types.REAL, 4),
        Arguments.of(Types.INTEGER, 4),
        Arguments.of(Types.FLOAT, 8),
        Arguments.of(Types.DOUBLE, 8),
        Arguments.of(Types.BIGINT, 8),
        Arguments.of(Types.BINARY, 32767),
        Arguments.of(Types.BIT, 1),
        Arguments.of(Types.BOOLEAN, 1),
        Arguments.of(Types.TINYINT, 1),
        Arguments.of(Types.SMALLINT, 2),
        Arguments.of(999, 0) // default case
        );
  }

  private static Stream<Arguments> charOctetArguments() {
    return Stream.of(
        Arguments.of("VARCHAR(100)", 100),
        Arguments.of("VARCHAR", 255),
        Arguments.of("CHAR(255)", 255),
        Arguments.of("CHAR", 255),
        Arguments.of("CHAR(123)", 123),
        Arguments.of("TEXT", 255),
        Arguments.of("VARCHAR(", 0),
        Arguments.of("VARCHAR(100,200)", 100),
        Arguments.of("VARCHAR(50,30)", 50),
        Arguments.of("INT", 0),
        Arguments.of("VARCHAR()", 0),
        Arguments.of("VARCHAR(abc)", 0));
  }

  private static Stream<Arguments> stripTypeNameArguments() {
    return Stream.of(
        Arguments.of("VARCHAR(100)", "VARCHAR"),
        Arguments.of("VARCHAR", "VARCHAR"),
        Arguments.of("CHAR(255)", "CHAR"),
        Arguments.of("TEXT", "TEXT"),
        Arguments.of("VARCHAR(", "VARCHAR"),
        Arguments.of("VARCHAR(100,200)", "VARCHAR"),
        Arguments.of("CHAR(123)", "CHAR"),
        Arguments.of("ARRAY<DOUBLE>", "ARRAY<DOUBLE>"),
        Arguments.of("MAP<STRING,ARRAY<INT>>", "MAP<STRING,ARRAY<INT>>"),
        Arguments.of("STRUCT<A:INT,B:STRING>", "STRUCT<A:INT,B:STRING>"),
        Arguments.of("ARRAY<DOUBLE>(100)", "ARRAY<DOUBLE>"),
        Arguments.of("MAP<STRING,INT>(50)", "MAP<STRING,INT>"),
        Arguments.of(null, null),
        Arguments.of("", ""),
        Arguments.of("INTEGER(10,5)", "INTEGER"));
  }

  private static Stream<Arguments> getBufferLengthArguments() {
    return Stream.of(
        // Null or empty typeVal
        Arguments.of(null, 0),
        Arguments.of("", 0),

        // Simple types without length specification
        Arguments.of("DATE", 6),
        Arguments.of("TIMESTAMP", 16),
        Arguments.of("BINARY", 32767),
        Arguments.of("STRING", 255),
        Arguments.of("INT", 4),

        // Types with length specification
        Arguments.of("CHAR(10)", 10),
        Arguments.of("VARCHAR(50)", 50),
        Arguments.of("DECIMAL(10,2)", 40),
        Arguments.of("NUMERIC(20)", 40),

        // Types without length but still valid strings
        Arguments.of("CHAR", 255),
        Arguments.of("VARCHAR", 255),
        Arguments.of("TEXT", 255));
  }

  private static Stream<Arguments> extractPrecisionArguments() {
    return Stream.of(
        Arguments.of("DECIMAL(100)", 100),
        Arguments.of("DECIMAL", 10),
        Arguments.of("DECIMAL(5,2)", 5));
  }

  private static Stream<Arguments> getSizeFromTypeValArguments() {
    return Stream.of(
        Arguments.of("VARCHAR(100)", 100),
        Arguments.of("VARCHAR", -1),
        Arguments.of("char(10)", 10),
        Arguments.of("", -1));
  }

  private static Stream<Arguments> getRowsTableTypeColumnArguments() {
    return Stream.of(
        Arguments.of("TABLE", "TABLE"),
        Arguments.of("VIEW", "VIEW"),
        Arguments.of("SYSTEM TABLE", "SYSTEM TABLE"),
        Arguments.of("", "TABLE"));
  }

  private static Stream<Arguments> provideSpecialColumnsArguments() {
    return Stream.of(
        Arguments.of(List.of("INTEGER", "", "", 0, ""), Arrays.asList("INTEGER", 4, null, 1, null)),
        Arguments.of(List.of("DATE", "", "", 1, ""), Arrays.asList("DATE", 91, 91, 2, null)));
  }

  private static Stream<Arguments> provideColumnSizeArguments() {
    return Stream.of(
        Arguments.of(List.of("VARCHAR(50)", 0, 0), List.of("VARCHAR", 50, 0)),
        Arguments.of(List.of("INT", 4, 10), List.of("INT", 10, 10)),
        Arguments.of(List.of("VARCHAR", 0, 0), List.of("VARCHAR", 255, 0)));
  }

  @ParameterizedTest
  @MethodSource("provideSqlTypesAndExpectedSizes")
  void testGetSizeInBytes(int sqlType, int expectedSize) {
    int actualSize = MetadataResultSetBuilder.getSizeInBytes(sqlType);
    assertEquals(expectedSize, actualSize);
  }

  @ParameterizedTest
  @MethodSource("getRowsTableTypeColumnArguments")
  void testGetRowsHandlesTableTypeColumn(String tableTypeValue, String expectedTableType)
      throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getObject(TABLE_TYPE_COLUMN.getResultSetColumnName()))
        .thenReturn(tableTypeValue);

    List<List<Object>> rows = MetadataResultSetBuilder.getRows(resultSet, TABLE_COLUMNS);

    assertEquals(expectedTableType, rows.get(0).get(3));
    assertEquals(String.class, rows.get(0).get(3).getClass());
  }

  private static Stream<Arguments> getRowsNullableColumnArguments() {
    return Stream.of(Arguments.of("true", 1), Arguments.of("false", 0), Arguments.of(null, 1));
  }

  private static Stream<Arguments> getRowsColumnTypeArguments() {
    return Stream.of(
        Arguments.of("INT", "INT"),
        Arguments.of("DECIMAL", "DECIMAL"),
        Arguments.of("DECIMAL(6,2)", "DECIMAL"),
        Arguments.of("MAP<STRING, ARRAY<STRING>>", "MAP<STRING, ARRAY<STRING>>"),
        Arguments.of("ARRAY<DOUBLE>", "ARRAY<DOUBLE>"));
  }

  @ParameterizedTest
  @MethodSource("getRowsNullableColumnArguments")
  void testGetRowsHandlesNullableColumn(String isNullableValue, int expectedNullable)
      throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getObject(IS_NULLABLE_COLUMN.getResultSetColumnName()))
        .thenReturn(isNullableValue);

    List<List<Object>> rows = MetadataResultSetBuilder.getRows(resultSet, COLUMN_COLUMNS);

    assertEquals(expectedNullable, rows.get(0).get(10));
    assertEquals(
        Integer.class, rows.get(0).get(10).getClass()); // test column type of nullable column
  }

  @ParameterizedTest
  @MethodSource("getRowsColumnTypeArguments")
  void testGetRowsColumnType(String typeName, String expectedTypeName) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getString(COLUMN_TYPE_COLUMN.getResultSetColumnName())).thenReturn(typeName);

    List<List<Object>> rows = MetadataResultSetBuilder.getRows(resultSet, COLUMN_COLUMNS);

    assertEquals(expectedTypeName, rows.get(0).get(5));
  }

  @Test
  void testGetThriftRowsWithRowIndexOutOfBounds() {
    List<ResultColumn> columns = List.of(COLUMN_TYPE_COLUMN, COL_NAME_COLUMN);
    List<Object> row = List.of("VARCHAR(50)");
    List<List<Object>> rows = List.of(row);

    List<List<Object>> updatedRows = MetadataResultSetBuilder.getThriftRows(rows, columns);
    List<Object> updatedRow = updatedRows.get(0);
    assertEquals("VARCHAR", updatedRow.get(0));
    assertNull(updatedRow.get(1));
  }

  @ParameterizedTest
  @MethodSource("provideSpecialColumnsArguments")
  void testGetThriftRowsSpecialColumns(List<Object> row, List<Object> expectedRow) {
    List<ResultColumn> columns =
        List.of(
            COLUMN_TYPE_COLUMN,
            SQL_DATA_TYPE_COLUMN,
            SQL_DATETIME_SUB_COLUMN,
            ORDINAL_POSITION_COLUMN,
            SCOPE_CATALOG_COLUMN);

    List<List<Object>> updatedRows = MetadataResultSetBuilder.getThriftRows(List.of(row), columns);
    List<Object> updatedRow = updatedRows.get(0);
    // verify following
    // 1. ordinal position is 1, 2
    // 2. sql data type is 4, 91
    // 3. sql_date_time_sub is null, 91
    // 4. scope_catalog_col is null, null
    assertEquals(expectedRow.get(1), updatedRow.get(1));
    assertEquals(expectedRow.get(2), updatedRow.get(2));
    assertEquals(expectedRow.get(3), updatedRow.get(3));
    assertEquals(expectedRow.get(4), updatedRow.get(4));
  }

  @ParameterizedTest
  @MethodSource("provideColumnSizeArguments")
  void testGetThriftRowsColumnSize(List<Object> row, List<Object> expectedRow) {
    List<ResultColumn> columns =
        List.of(COLUMN_TYPE_COLUMN, COLUMN_SIZE_COLUMN, NUM_PREC_RADIX_COLUMN);

    List<List<Object>> updatedRows = MetadataResultSetBuilder.getThriftRows(List.of(row), columns);
    List<Object> updatedRow = updatedRows.get(0);

    assertEquals(expectedRow.get(0), updatedRow.get(0));
    assertEquals(expectedRow.get(1), updatedRow.get(1));
  }

  @ParameterizedTest
  @MethodSource("extractPrecisionArguments")
  public void testExtractPrecision(String typeVal, int expected) {
    int actual = MetadataResultSetBuilder.extractPrecision(typeVal);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("getBufferLengthArguments")
  public void testGetBufferLength(String typeVal, int expected) {
    int actual = MetadataResultSetBuilder.getBufferLength(typeVal);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("getSizeFromTypeValArguments")
  public void testGetSizeFromTypeVal(String typeVal, int expected) {
    int actual = MetadataResultSetBuilder.getSizeFromTypeVal(typeVal);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("stripTypeNameArguments")
  public void testStripTypeName(String input, String expected) {
    String actual = MetadataResultSetBuilder.stripTypeName(input);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("charOctetArguments")
  public void testGetCharOctetLength(String typeVal, int expected) {
    System.out.println("Incorrect getCharOctet for typeVal : " + typeVal);
    int actual = MetadataResultSetBuilder.getCharOctetLength(typeVal);
    assertEquals(expected, actual);
  }

  @Test
  void testGetTablesResultFilteringByTableTypes() throws SQLException {
    // Create sample rows with different table types
    List<List<Object>> rows = new ArrayList<>();
    rows.add(Arrays.asList("catalog1", "schema1", "table1", "TABLE", "comment1"));
    rows.add(Arrays.asList("catalog1", "schema1", "view1", "VIEW", "comment2"));
    rows.add(Arrays.asList("catalog1", "schema1", "system1", "SYSTEM TABLE", "comment3"));

    // Test filtering to include only TABLE type
    String[] tableTypes = new String[] {"TABLE"};
    ResultSet resultSet = MetadataResultSetBuilder.getTablesResult("catalog1", tableTypes, rows);

    // Verify that only the row with table type "TABLE" is included
    assertTrue(resultSet.next());
    assertEquals("table1", resultSet.getString("TABLE_NAME"));
    assertEquals("TABLE", resultSet.getString("TABLE_TYPE"));
    assertEquals("comment1", resultSet.getString("REMARKS"));
    assertEquals("schema1", resultSet.getString("TABLE_SCHEM"));
    assertEquals("catalog1", resultSet.getString("TABLE_CAT"));
    assertFalse(resultSet.next());
  }

  @Test
  void testGetTablesResultWithNullTableType() throws SQLException {
    // Create rows with null or empty table types
    List<List<Object>> rows = new ArrayList<>();
    rows.add(Arrays.asList("catalog1", "schema1", "table1", null, "comment1"));
    rows.add(Arrays.asList("catalog1", "schema1", "table2", "", "comment2"));

    // Test filtering to include only TABLE type
    String[] tableTypes = new String[] {"TABLE"};
    ResultSet resultSet = MetadataResultSetBuilder.getTablesResult("catalog1", tableTypes, rows);

    // Verify rows with null/empty table type are converted to "TABLE" and included
    int rowCount = 0;
    while (resultSet.next()) {
      rowCount++;
      assertEquals("TABLE", resultSet.getString("TABLE_TYPE"));
    }
    assertEquals(2, rowCount);
  }
}
