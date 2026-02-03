package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.common.MetadataResultConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MetadataResultSetBuilderTest {

  @Mock private IDatabricksConnectionContext connectionContext;
  private MetadataResultSetBuilder metadataResultSetBuilder;

  @BeforeEach
  void setUp() {
    metadataResultSetBuilder = new MetadataResultSetBuilder(connectionContext);
  }

  @AfterEach
  void tearDown() {
    DatabricksThreadContextHolder.clearAllContext();
  }

  @Test
  void testGetCode() {
    assert metadataResultSetBuilder.getCode("STRING") == 12;
    assert metadataResultSetBuilder.getCode("INT") == 4;
    assert metadataResultSetBuilder.getCode("DOUBLE") == 8;
    assert metadataResultSetBuilder.getCode("FLOAT") == 6;
    assert metadataResultSetBuilder.getCode("BOOLEAN") == 16;
    assert metadataResultSetBuilder.getCode("DATE") == 91;
    assert metadataResultSetBuilder.getCode("TIMESTAMP_NTZ") == 93;
    assert metadataResultSetBuilder.getCode("TIMESTAMP") == 93;
    assert metadataResultSetBuilder.getCode("DECIMAL") == 3;
    assert metadataResultSetBuilder.getCode("BINARY") == -2;
    assert metadataResultSetBuilder.getCode("ARRAY") == 2003;
    assert metadataResultSetBuilder.getCode("MAP") == 2002;
    assert metadataResultSetBuilder.getCode("STRUCT") == 2002;
    assert metadataResultSetBuilder.getCode("UNIONTYPE") == 2002;
    assert metadataResultSetBuilder.getCode("BYTE") == -6;
    assert metadataResultSetBuilder.getCode("SHORT") == 5;
    assert metadataResultSetBuilder.getCode("LONG") == -5;
    assert metadataResultSetBuilder.getCode("NULL") == 0;
    assert metadataResultSetBuilder.getCode("VOID") == 0;
    assert metadataResultSetBuilder.getCode("CHAR") == 1;
    assert metadataResultSetBuilder.getCode("VARCHAR") == 12;
    assert metadataResultSetBuilder.getCode("CHARACTER") == 1;
    assert metadataResultSetBuilder.getCode("BIGINT") == -5;
    assert metadataResultSetBuilder.getCode("TINYINT") == -6;
    assert metadataResultSetBuilder.getCode("SMALLINT") == 5;
    assert metadataResultSetBuilder.getCode("INTEGER") == 4;
    assert metadataResultSetBuilder.getCode("VARIANT") == 1111;
    assert metadataResultSetBuilder.getCode("INTERVAL") == 12;
    assert metadataResultSetBuilder.getCode("INTERVAL YEAR") == 12;
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
        Arguments.of("CHAR(255)", 255),
        Arguments.of("CHAR(123)", 123),
        Arguments.of("VARCHAR(", 0),
        Arguments.of("VARCHAR(100,200)", 100),
        Arguments.of("VARCHAR(50,30)", 50),
        Arguments.of("INT", 0),
        Arguments.of("VARCHAR()", 0),
        Arguments.of("VARCHAR(abc)", 0),
        Arguments.of("INTERVAL YEAR", 0));
  }

  private static Stream<Arguments> charOctetArgumentsVarchar() {
    return Stream.of(
        Arguments.of("VARCHAR", 255), Arguments.of("CHAR", 255), Arguments.of("TEXT", 255));
  }

  private static Stream<Arguments> stripTypeNameArguments() {
    return Stream.of(
        Arguments.of("VARCHAR(100)", "VARCHAR"),
        Arguments.of("VARCHAR", "VARCHAR"),
        Arguments.of("CHAR(255)", "CHAR"),
        Arguments.of("TEXT", "TEXT"),
        Arguments.of("VARCHAR()", "VARCHAR"),
        Arguments.of("VARCHAR(100,200)", "VARCHAR"),
        Arguments.of("CHAR(123)", "CHAR"),
        Arguments.of("ARRAY<DOUBLE>", "ARRAY<DOUBLE>"),
        Arguments.of("MAP<STRING,ARRAY<INT>>", "MAP<STRING,ARRAY<INT>>"),
        Arguments.of("STRUCT<A:INT,B:STRING>", "STRUCT<A:INT,B:STRING>"),
        Arguments.of("ARRAY<DOUBLE>(100)", "ARRAY<DOUBLE>"),
        Arguments.of("MAP<STRING,INT>(50)", "MAP<STRING,INT>"),
        Arguments.of(null, null),
        Arguments.of("", ""),
        Arguments.of("INTEGER(10,5)", "INTEGER"),
        Arguments.of("ARRAY<DECIMAL(10,2)>", "ARRAY<DECIMAL(10,2)>"));
  }

  private static Stream<Arguments> stripBaseTypeNameArguments() {
    return Stream.of(
        Arguments.of("VARCHAR(100)", "VARCHAR"),
        Arguments.of("VARCHAR", "VARCHAR"),
        Arguments.of("CHAR(255)", "CHAR"),
        Arguments.of("TEXT", "TEXT"),
        Arguments.of("VARCHAR(", "VARCHAR"),
        Arguments.of("VARCHAR(100,200)", "VARCHAR"),
        Arguments.of("CHAR(123)", "CHAR"),
        Arguments.of("ARRAY<DOUBLE>", "ARRAY"),
        Arguments.of("MAP<STRING,ARRAY<INT>>", "MAP"),
        Arguments.of("STRUCT<A:INT,B:STRING>", "STRUCT"),
        Arguments.of("ARRAY<DOUBLE>(100)", "ARRAY"),
        Arguments.of("MAP<STRING,INT>(50)", "MAP"),
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
        Arguments.of("INT", 4),

        // Types with length specification
        Arguments.of("CHAR(10)", 10),
        Arguments.of("VARCHAR(50)", 50),
        Arguments.of("DECIMAL(10,2)", 40),
        Arguments.of("NUMERIC(20)", 40));
  }

  private static Stream<Arguments> getBufferLengthArgumentsVarchar() {
    return Stream.of(
        Arguments.of("STRING", 255),
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
        Arguments.of(List.of("INT", 4, 10), List.of("INT", 10, 10)));
  }

  private static Stream<Arguments> provideColumnSizeArgumentsVarchar() {
    return Stream.of(Arguments.of(List.of("VARCHAR", 0, 0), List.of("VARCHAR", 255, 0)));
  }

  @ParameterizedTest
  @MethodSource("provideSqlTypesAndExpectedSizes")
  void testGetSizeInBytes(int sqlType, int expectedSize) {
    int actualSize = metadataResultSetBuilder.getSizeInBytes(sqlType);
    assertEquals(expectedSize, actualSize);
  }

  @ParameterizedTest
  @MethodSource("getRowsTableTypeColumnArguments")
  void testGetRowsHandlesTableTypeColumn(String tableTypeValue, String expectedTableType)
      throws SQLException {
    DatabricksResultSet resultSet = mock(DatabricksResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    for (ResultColumn resultColumn : TABLE_COLUMNS) {
      when(resultSet.getObject(resultColumn.getResultSetColumnName())).thenReturn(null);
    }
    when(resultSet.getObject(TABLE_TYPE_COLUMN.getResultSetColumnName()))
        .thenReturn(tableTypeValue);

    List<List<Object>> rows =
        metadataResultSetBuilder.getRows(
            resultSet, TABLE_COLUMNS, new DefaultDatabricksResultSetAdapter());

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
        Arguments.of("ARRAY<DOUBLE>", "ARRAY<DOUBLE>"),
        Arguments.of("BINGINT measure", "BINGINT measure"));
  }

  @ParameterizedTest
  @MethodSource("getRowsNullableColumnArguments")
  void testGetRowsHandlesNullableColumn(String isNullableValue, int expectedNullable)
      throws SQLException {
    DatabricksResultSet resultSet = mock(DatabricksResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    for (ResultColumn resultColumn : COLUMN_COLUMNS) {
      if (resultColumn.getResultSetColumnName().equals("SQLDataType")) {
        // Special handling begins from SQLDataType columns onward; getObject is no longer invoked.
        break;
      }
      when(resultSet.getObject(resultColumn.getResultSetColumnName())).thenReturn(null);
    }
    when(resultSet.getObject(IS_NULLABLE_COLUMN.getResultSetColumnName()))
        .thenReturn(isNullableValue);

    List<List<Object>> rows =
        metadataResultSetBuilder.getRows(
            resultSet, COLUMN_COLUMNS, new DefaultDatabricksResultSetAdapter());

    assertEquals(expectedNullable, rows.get(0).get(10));
    assertEquals(
        Integer.class, rows.get(0).get(10).getClass()); // test column type of nullable column
  }

  @ParameterizedTest
  @MethodSource("getRowsColumnTypeArguments")
  void testGetRowsColumnType(String typeName, String expectedTypeName) throws SQLException {
    DatabricksResultSet resultSet = mock(DatabricksResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getString(COLUMN_TYPE_COLUMN.getResultSetColumnName())).thenReturn(typeName);

    List<List<Object>> rows =
        metadataResultSetBuilder.getRows(
            resultSet, COLUMN_COLUMNS, new DefaultDatabricksResultSetAdapter());

    assertEquals(expectedTypeName, rows.get(0).get(5));
  }

  @Test
  void testGetThriftRowsWithRowIndexOutOfBounds() {
    List<ResultColumn> columns = List.of(COLUMN_TYPE_COLUMN, COL_NAME_COLUMN);
    List<Object> row = List.of("VARCHAR(50)");
    List<List<Object>> rows = List.of(row);

    List<List<Object>> updatedRows = metadataResultSetBuilder.getThriftRows(rows, columns);
    List<Object> updatedRow = updatedRows.get(0);
    assertEquals("VARCHAR", updatedRow.get(0));
    assertNull(updatedRow.get(1));
  }

  @Test
  void testGetThriftRowsMeasureColumn() {
    List<ResultColumn> columns = List.of(COLUMN_TYPE_COLUMN);
    List<Object> row = List.of("DECIMAL(6,2) measure");
    List<List<Object>> updatedRows = metadataResultSetBuilder.getThriftRows(List.of(row), columns);
    List<Object> updatedRow = updatedRows.get(0);
    // verify that type name for measure column is not stripped
    assertEquals("DECIMAL(6,2) measure", updatedRow.get(0));
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

    List<List<Object>> updatedRows = metadataResultSetBuilder.getThriftRows(List.of(row), columns);
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

    List<List<Object>> updatedRows = metadataResultSetBuilder.getThriftRows(List.of(row), columns);
    List<Object> updatedRow = updatedRows.get(0);

    assertEquals(expectedRow.get(0), updatedRow.get(0));
    assertEquals(expectedRow.get(1), updatedRow.get(1));
  }

  @ParameterizedTest
  @MethodSource("provideColumnSizeArgumentsVarchar")
  void testGetThriftRowsColumnSizeVarchar(List<Object> row, List<Object> expectedRow) {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(context.getDefaultStringColumnLength()).thenReturn(255);
    MetadataResultSetBuilder metadataResultSetBuilder = new MetadataResultSetBuilder(context);
    List<ResultColumn> columns =
        List.of(COLUMN_TYPE_COLUMN, COLUMN_SIZE_COLUMN, NUM_PREC_RADIX_COLUMN);

    List<List<Object>> updatedRows = metadataResultSetBuilder.getThriftRows(List.of(row), columns);
    List<Object> updatedRow = updatedRows.get(0);

    assertEquals(expectedRow.get(0), updatedRow.get(0));
    assertEquals(expectedRow.get(1), updatedRow.get(1));
  }

  @ParameterizedTest
  @MethodSource("extractPrecisionArguments")
  public void testExtractPrecision(String typeVal, int expected) {
    int actual = metadataResultSetBuilder.extractPrecision(typeVal);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("getBufferLengthArguments")
  public void testGetBufferLength(String typeVal, int expected) {
    int actual = metadataResultSetBuilder.getBufferLength(typeVal);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("getBufferLengthArgumentsVarchar")
  public void testGetBufferLengthVarchar(String typeVal, int expected) {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(context.getDefaultStringColumnLength()).thenReturn(255);
    MetadataResultSetBuilder metadataResultSetBuilder1 = new MetadataResultSetBuilder(context);

    int actual = metadataResultSetBuilder1.getBufferLength(typeVal);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("getSizeFromTypeValArguments")
  public void testGetSizeFromTypeVal(String typeVal, int expected) {
    int actual = metadataResultSetBuilder.getSizeFromTypeVal(typeVal);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("stripTypeNameArguments")
  public void testStripTypeName(String input, String expected) {
    String actual = metadataResultSetBuilder.stripTypeName(input);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("stripBaseTypeNameArguments")
  public void testStripBaseTypeName(String input, String expected) {
    String actual = metadataResultSetBuilder.stripBaseTypeName(input);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("charOctetArguments")
  public void testGetCharOctetLength(String typeVal, int expected) {
    int actual = metadataResultSetBuilder.getCharOctetLength(typeVal);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("charOctetArgumentsVarchar")
  public void testGetCharOctetLengthVarchar(String typeVal, int expected) {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(context.getDefaultStringColumnLength()).thenReturn(255);
    MetadataResultSetBuilder metadataResultSetBuilder = new MetadataResultSetBuilder(context);
    int actual = metadataResultSetBuilder.getCharOctetLength(typeVal);
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
    ResultSet resultSet = metadataResultSetBuilder.getTablesResult("catalog1", tableTypes, rows);

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
    ResultSet resultSet = metadataResultSetBuilder.getTablesResult("catalog1", tableTypes, rows);

    // Verify rows with null/empty table type are converted to "TABLE" and included
    int rowCount = 0;
    while (resultSet.next()) {
      rowCount++;
      assertEquals("TABLE", resultSet.getString("TABLE_TYPE"));
    }
    assertEquals(2, rowCount);
  }

  @Test
  void testGetTablesResultSortingInSeaMode() throws SQLException {
    // Create mock DatabricksResultSet with rows in unsorted order
    DatabricksResultSet mockResultSet = mock(DatabricksResultSet.class, withSettings().lenient());
    java.sql.ResultSetMetaData mockMetaData = mock(java.sql.ResultSetMetaData.class);
    when(mockMetaData.getColumnCount()).thenReturn(TABLE_COLUMNS.size());
    when(mockResultSet.getMetaData()).thenReturn(mockMetaData);

    // Return 4 rows in unsorted order, then false
    when(mockResultSet.next())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);

    // Stub all TABLE_COLUMNS to return null by default (some columns aren't in resultset)
    for (ResultColumn resultColumn : TABLE_COLUMNS) {
      when(mockResultSet.getObject(resultColumn.getResultSetColumnName())).thenReturn(null);
    }

    // Set up rows in deliberately unsorted order to verify sorting works
    // Expected sort order: TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME
    // Row data uses ResultColumn.getResultSetColumnName() values
    when(mockResultSet.getObject(CATALOG_COLUMN.getResultSetColumnName())) // TABLE_CAT
        .thenReturn("catalog_b")
        .thenReturn("catalog_a")
        .thenReturn("catalog_a")
        .thenReturn("catalog_a");
    when(mockResultSet.getObject(SCHEMA_COLUMN.getResultSetColumnName())) // TABLE_SCHEM
        .thenReturn("schema_z")
        .thenReturn("schema_a")
        .thenReturn("schema_b")
        .thenReturn("schema_a");
    when(mockResultSet.getObject(TABLE_NAME_COLUMN.getResultSetColumnName())) // TABLE_NAME
        .thenReturn("table_x")
        .thenReturn("table_b")
        .thenReturn("table_a")
        .thenReturn("table_a");
    when(mockResultSet.getObject(TABLE_TYPE_COLUMN.getResultSetColumnName())) // TABLE_TYPE
        .thenReturn("VIEW")
        .thenReturn("TABLE")
        .thenReturn("TABLE")
        .thenReturn("VIEW");

    // Call SEA mode method with both TABLE and VIEW types
    String[] tableTypes = new String[] {"TABLE", "VIEW"};
    ResultSet resultSet = metadataResultSetBuilder.getTablesResult(mockResultSet, tableTypes);

    // Verify sorting: TABLE_TYPE first, then TABLE_CAT, TABLE_SCHEM, TABLE_NAME
    // Expected order after sorting:
    // 1. TABLE, catalog_a, schema_a, table_b
    // 2. TABLE, catalog_a, schema_b, table_a
    // 3. VIEW, catalog_a, schema_a, table_a
    // 4. VIEW, catalog_b, schema_z, table_x

    assertTrue(resultSet.next());
    assertEquals("TABLE", resultSet.getString("TABLE_TYPE"));
    assertEquals("catalog_a", resultSet.getString("TABLE_CAT"));
    assertEquals("schema_a", resultSet.getString("TABLE_SCHEM"));
    assertEquals("table_b", resultSet.getString("TABLE_NAME"));

    assertTrue(resultSet.next());
    assertEquals("TABLE", resultSet.getString("TABLE_TYPE"));
    assertEquals("catalog_a", resultSet.getString("TABLE_CAT"));
    assertEquals("schema_b", resultSet.getString("TABLE_SCHEM"));
    assertEquals("table_a", resultSet.getString("TABLE_NAME"));

    assertTrue(resultSet.next());
    assertEquals("VIEW", resultSet.getString("TABLE_TYPE"));
    assertEquals("catalog_a", resultSet.getString("TABLE_CAT"));
    assertEquals("schema_a", resultSet.getString("TABLE_SCHEM"));
    assertEquals("table_a", resultSet.getString("TABLE_NAME"));

    assertTrue(resultSet.next());
    assertEquals("VIEW", resultSet.getString("TABLE_TYPE"));
    assertEquals("catalog_b", resultSet.getString("TABLE_CAT"));
    assertEquals("schema_z", resultSet.getString("TABLE_SCHEM"));
    assertEquals("table_x", resultSet.getString("TABLE_NAME"));

    assertFalse(resultSet.next());
  }

  @Test
  void testComplexTypesReturnVarcharWhenSupportDisabled() throws SQLException {
    when(connectionContext.isComplexDatatypeSupportEnabled()).thenReturn(false);

    List<ResultColumn> columns =
        Arrays.asList(DATA_TYPE_COLUMN, COLUMN_TYPE_COLUMN, SQL_DATA_TYPE_COLUMN);
    List<List<Object>> rows =
        Arrays.asList(
            Arrays.asList(2003, "ARRAY<DATE>", 2003),
            Arrays.asList(2002, "MAP<STRING,INT>", 2002),
            Arrays.asList(2002, "STRUCT<name:STRING,age:INT>", 2002),
            Arrays.asList(1111, "VARIANT", 1111));

    List<List<Object>> updatedRows = metadataResultSetBuilder.getThriftRows(rows, columns);

    List<Object> arrayRow = updatedRows.get(0);
    assertEquals(12, arrayRow.get(0));
    assertEquals(12, arrayRow.get(2));

    List<Object> mapRow = updatedRows.get(1);
    assertEquals(12, mapRow.get(0));
    assertEquals(12, mapRow.get(2));

    List<Object> structRow = updatedRows.get(2);
    assertEquals(12, structRow.get(0));
    assertEquals(12, structRow.get(2));

    List<Object> variantRow = updatedRows.get(3);
    assertEquals(1111, variantRow.get(0));
    assertEquals(1111, variantRow.get(2));
  }

  @Test
  void testComplexTypesReturnActualCodesWhenSupportEnabled() throws SQLException {
    when(connectionContext.isComplexDatatypeSupportEnabled()).thenReturn(true);

    List<ResultColumn> columns =
        Arrays.asList(DATA_TYPE_COLUMN, COLUMN_TYPE_COLUMN, SQL_DATA_TYPE_COLUMN);
    List<List<Object>> rows =
        Arrays.asList(
            Arrays.asList(2003, "ARRAY<DATE>", 2003),
            Arrays.asList(2002, "MAP<STRING,INT>", 2002),
            Arrays.asList(2002, "STRUCT<name:STRING,age:INT>", 2002),
            Arrays.asList(1111, "VARIANT", 1111));

    List<List<Object>> updatedRows = metadataResultSetBuilder.getThriftRows(rows, columns);

    List<Object> arrayRow = updatedRows.get(0);
    assertEquals(2003, arrayRow.get(0));
    assertEquals(2003, arrayRow.get(2));

    List<Object> mapRow = updatedRows.get(1);
    assertEquals(2002, mapRow.get(0));
    assertEquals(2002, mapRow.get(2));

    List<Object> structRow = updatedRows.get(2);
    assertEquals(2002, structRow.get(0));
    assertEquals(2002, structRow.get(2));

    List<Object> variantRow = updatedRows.get(3);
    assertEquals(1111, variantRow.get(0));
    assertEquals(1111, variantRow.get(2));
  }

  @Test
  void testDecimalDigitsColumnInGetThriftRows() {
    List<ResultColumn> columns = Arrays.asList(COLUMN_TYPE_COLUMN, DECIMAL_DIGITS_COLUMN);

    List<List<Object>> rows =
        Arrays.asList(
            Arrays.asList("DECIMAL(10,2)", 2),
            Arrays.asList("TIMESTAMP", 6),
            Arrays.asList("INT", 0),
            Arrays.asList("VARCHAR(100)", 0),
            Arrays.asList("DECIMAL(15,5)", 5),
            Arrays.asList("TIMESTAMP_NTZ", 6),
            Arrays.asList("DECIMAL", 0));

    List<List<Object>> updatedRows = metadataResultSetBuilder.getThriftRows(rows, columns);

    assertEquals(2, updatedRows.get(0).get(1), "DECIMAL(10,2) should have scale 2");
    assertEquals(9, updatedRows.get(1).get(1), "TIMESTAMP should have scale 9");
    assertEquals(0, updatedRows.get(2).get(1), "INT should have scale 0");
    assertEquals(0, updatedRows.get(3).get(1), "VARCHAR should have scale 0");
    assertEquals(5, updatedRows.get(4).get(1), "DECIMAL(15,5) should have scale 5");
    assertEquals(9, updatedRows.get(5).get(1), "TIMESTAMP_NTZ should have scale 9");
    assertEquals(0, updatedRows.get(6).get(1), "DECIMAL should have scale 0");
  }

  private static Stream<Arguments> provideDecimalDigitsArguments() {
    return Stream.of(
        Arguments.of("DECIMAL(10,2)", 2, 2, "DECIMAL(10,2) should have scale 2"),
        Arguments.of("TIMESTAMP", 6, 9, "TIMESTAMP should have scale 9"),
        Arguments.of("INT", 0, 0, "INT should have scale 0"),
        Arguments.of("VARCHAR(100)", 0, 0, "VARCHAR should have scale 0"),
        Arguments.of("DECIMAL(15,5)", 5, 5, "DECIMAL(15,5) should have scale 5"),
        Arguments.of("TIMESTAMP_NTZ", 6, 9, "TIMESTAMP_NTZ should have scale 9"),
        Arguments.of("DECIMAL", 0, 0, "DECIMAL should have scale 0"));
  }

  @ParameterizedTest
  @MethodSource("provideDecimalDigitsArguments")
  void testDecimalDigitsColumnInGetRows(
      String typeName, int inputScale, int expectedScale, String message) throws SQLException {
    DatabricksResultSet resultSet = mock(DatabricksResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(false);

    for (ResultColumn resultColumn : COLUMN_COLUMNS) {
      if (resultColumn.getResultSetColumnName().equals("SQLDataType")) {
        // Special handling begins from SQLDataType columns onward; getObject is no longer invoked.
        break;
      }
      when(resultSet.getObject(resultColumn.getResultSetColumnName())).thenReturn(null);
    }
    when(resultSet.getObject(IS_NULLABLE_COLUMN.getResultSetColumnName())).thenReturn("true");
    when(resultSet.getString(COLUMN_TYPE_COLUMN.getResultSetColumnName())).thenReturn(typeName);
    when(resultSet.getObject(DECIMAL_DIGITS_COLUMN.getResultSetColumnName()))
        .thenReturn(inputScale);

    List<List<Object>> rows =
        metadataResultSetBuilder.getRows(
            resultSet, COLUMN_COLUMNS, new DefaultDatabricksResultSetAdapter());

    assertEquals(expectedScale, rows.get(0).get(8), message);
  }

  @Test
  void testGetTableTypesResultWithMetricViewEnabled() throws SQLException {
    // Test when EnableMetricViewMetadata=true
    when(connectionContext.getEnableMetricViewMetadata()).thenReturn(true);

    DatabricksResultSet resultSet = metadataResultSetBuilder.getTableTypesResult();

    // Verify we get 4 table types including METRIC_VIEW
    List<String> tableTypes = new ArrayList<>();
    while (resultSet.next()) {
      tableTypes.add(resultSet.getString("TABLE_TYPE"));
    }

    assertEquals(4, tableTypes.size());
    assertTrue(tableTypes.contains("SYSTEM TABLE"));
    assertTrue(tableTypes.contains("TABLE"));
    assertTrue(tableTypes.contains("VIEW"));
    assertTrue(tableTypes.contains("METRIC_VIEW"));
  }

  @Test
  void testGetTableTypesResultWithMetricViewDisabled() throws SQLException {
    // Test when EnableMetricViewMetadata=false
    when(connectionContext.getEnableMetricViewMetadata()).thenReturn(false);

    DatabricksResultSet resultSet = metadataResultSetBuilder.getTableTypesResult();

    // Verify we get 3 table types without METRIC_VIEW
    List<String> tableTypes = new ArrayList<>();
    while (resultSet.next()) {
      tableTypes.add(resultSet.getString("TABLE_TYPE"));
    }

    assertEquals(3, tableTypes.size());
    assertTrue(tableTypes.contains("SYSTEM TABLE"));
    assertTrue(tableTypes.contains("TABLE"));
    assertTrue(tableTypes.contains("VIEW"));
    assertFalse(tableTypes.contains("METRIC_VIEW"));
  }

  @Test
  void shouldAllowCatalogAccessWhenMultipleCatalogSupportEnabled() throws SQLException {
    when(connectionContext.getEnableMultipleCatalogSupport()).thenReturn(true);
    IDatabricksSession session = mock(IDatabricksSession.class);

    assertTrue(metadataResultSetBuilder.shouldAllowCatalogAccess("cat_a", "cat_b", session));
    verify(session, never()).getCurrentCatalog();
  }

  @Test
  void shouldAllowCatalogAccessWhenCatalogIsNull() throws SQLException {
    when(connectionContext.getEnableMultipleCatalogSupport()).thenReturn(false);
    IDatabricksSession session = mock(IDatabricksSession.class);

    assertTrue(metadataResultSetBuilder.shouldAllowCatalogAccess(null, "cat_b", session));
    verify(session, never()).getCurrentCatalog();
  }

  @Test
  void shouldAllowCatalogAccessWhenCurrentCatalogMatchesSession() throws SQLException {
    when(connectionContext.getEnableMultipleCatalogSupport()).thenReturn(false);
    IDatabricksSession session = mock(IDatabricksSession.class);
    when(session.getCurrentCatalog()).thenReturn("main");

    assertTrue(metadataResultSetBuilder.shouldAllowCatalogAccess("main", null, session));
    verify(session).getCurrentCatalog();
  }

  @Test
  void shouldDenyCatalogAccessWhenCatalogDiffers() throws SQLException {
    when(connectionContext.getEnableMultipleCatalogSupport()).thenReturn(false);
    IDatabricksSession session = mock(IDatabricksSession.class);
    when(session.getCurrentCatalog()).thenReturn("main");

    assertFalse(metadataResultSetBuilder.shouldAllowCatalogAccess("other", null, session));
    verify(session).getCurrentCatalog();
  }
}
