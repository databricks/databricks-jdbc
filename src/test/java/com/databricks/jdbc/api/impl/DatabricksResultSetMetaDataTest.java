package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.Nullable.NULLABLE;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.TIMESTAMP;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.VARIANT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;

public class DatabricksResultSetMetaDataTest {
  private static final StatementId STATEMENT_ID = new StatementId("statementId");
  private static final StatementId THRIFT_STATEMENT_ID =
      StatementId.deserialize(
          "01efc77c-7c8b-1a8e-9ecb-a9a6e6aa050a|338d529d-8272-46eb-8482-cb419466839d");
  @Mock private IDatabricksConnectionContext connectionContext;

  @BeforeEach
  void setUp() {
    connectionContext = Mockito.mock(IDatabricksConnectionContext.class);
    when(connectionContext.getDefaultStringColumnLength()).thenReturn(255);
    DatabricksThreadContextHolder.setConnectionContext(connectionContext);
  }

  @AfterEach
  void tearDown() {
    DatabricksThreadContextHolder.clearAllContext();
  }

  static Stream<TSparkRowSetType> thriftResultFormats() {
    return Stream.of(
        TSparkRowSetType.ARROW_BASED_SET,
        TSparkRowSetType.COLUMN_BASED_SET,
        TSparkRowSetType.ROW_BASED_SET,
        TSparkRowSetType.URL_BASED_SET);
  }

  public ColumnInfo getColumn(String name, ColumnInfoTypeName typeName, String typeText) {
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setName(name);
    columnInfo.setTypeName(typeName);
    columnInfo.setTypeText(typeText);
    return columnInfo;
  }

  public ResultManifest getResultManifest() {
    ResultManifest manifest = new ResultManifest();
    manifest.setTotalRowCount(10L);
    ResultSchema schema = new ResultSchema();
    schema.setColumnCount(3L);
    ColumnInfo col1 = getColumn("col1", ColumnInfoTypeName.INT, "int");
    ColumnInfo col2 = getColumn("col2", ColumnInfoTypeName.STRING, "string");
    ColumnInfo col2dup = getColumn("col2", ColumnInfoTypeName.DOUBLE, "double");
    ColumnInfo col3 = getColumn("col5", null, "double");
    schema.setColumns(List.of(col1, col2, col2dup, col3));
    manifest.setSchema(schema);
    return manifest;
  }

  public TGetResultSetMetadataResp getThriftResultManifest() {
    TGetResultSetMetadataResp resultSetMetadataResp = new TGetResultSetMetadataResp();
    TColumnDesc columnDesc = new TColumnDesc().setColumnName("testCol");
    TTableSchema schema = new TTableSchema().setColumns(Collections.singletonList(columnDesc));
    resultSetMetadataResp.setSchema(schema);
    return resultSetMetadataResp;
  }

  @Test
  public void testColumnsWithSameNameAndNullTypeName() throws SQLException {
    ResultManifest resultManifest = getResultManifest();
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, false, connectionContext);
    assertEquals(4, metaData.getColumnCount());
    assertEquals("col1", metaData.getColumnName(1));
    assertEquals("col2", metaData.getColumnName(2));
    assertEquals("col2", metaData.getColumnName(3));
    assertEquals("col5", metaData.getColumnName(4));
    assertEquals(10, metaData.getTotalRows());
    assertEquals(2, metaData.getColumnNameIndex("col2"));
    assertEquals(2, metaData.getColumnNameIndex("COL2"));

    metaData =
        new DatabricksResultSetMetaData(
            STATEMENT_ID,
            List.of("col1", "col2", "col2"),
            List.of("int", "string", "double"),
            List.of(4, 12, 8),
            List.of(0, 0, 0),
            List.of(NULLABLE, NULLABLE, NULLABLE),
            10);
    assertEquals(3, metaData.getColumnCount());
    assertEquals("col1", metaData.getColumnName(1));
    assertEquals("col2", metaData.getColumnName(2));
    assertEquals("col2", metaData.getColumnName(3));
    assertEquals(10, metaData.getTotalRows());
    assertEquals(2, metaData.getColumnNameIndex("col2"));
  }

  @Test
  public void testColumnsWithTimestampNTZ() throws SQLException {
    ResultManifest resultManifest = new ResultManifest();
    resultManifest.setTotalRowCount(10L);
    ResultSchema schema = new ResultSchema();
    schema.setColumnCount(1L);

    ColumnInfo timestampColumnInfo = getColumn("timestamp_ntz", null, "TIMESTAMP_NTZ");
    schema.setColumns(List.of(timestampColumnInfo));
    resultManifest.setSchema(schema);

    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, false, connectionContext);
    assertEquals(1, metaData.getColumnCount());
    assertEquals("timestamp_ntz", metaData.getColumnName(1));
    assertEquals(TIMESTAMP, metaData.getColumnTypeName(1));
    assertEquals(Types.TIMESTAMP, metaData.getColumnType(1));
    assertEquals(10, metaData.getTotalRows());
  }

  @Test
  public void testDatabricksResultSetMetaDataInitialization() throws SQLException {
    // Instantiate the DatabricksResultSetMetaData
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(
            STATEMENT_ID,
            List.of("col1", "col2", "col3"),
            List.of("INTEGER", "VARCHAR", "DOUBLE"),
            new int[] {4, 12, 8},
            new int[] {10, 255, 15},
            new int[] {
              ResultSetMetaData.columnNullable,
              ResultSetMetaData.columnNoNulls,
              ResultSetMetaData.columnNullable
            },
            100);

    // Assertions to validate initialization
    assertEquals(3, metaData.getColumnCount());
    assertEquals(100, metaData.getTotalRows());

    // Validate column properties
    assertEquals("col1", metaData.getColumnName(1));
    assertEquals("col2", metaData.getColumnName(2));
    assertEquals("col3", metaData.getColumnName(3));
    assertEquals(4, metaData.getColumnType(1)); // INTEGER
    assertEquals(12, metaData.getColumnType(2)); // VARCHAR
    assertEquals(8, metaData.getColumnType(3)); // DOUBLE

    // Validate column type text and precision
    assertEquals("INTEGER", metaData.getColumnTypeName(1));
    assertEquals("VARCHAR", metaData.getColumnTypeName(2));
    assertEquals("DOUBLE", metaData.getColumnTypeName(3));
    assertEquals(10, metaData.getPrecision(1));
    assertEquals(255, metaData.getPrecision(2));
    assertEquals(15, metaData.getPrecision(3));

    assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(1));
    assertEquals(ResultSetMetaData.columnNoNulls, metaData.isNullable(2));
    assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(3));
  }

  @Test
  public void testDatabricksResultSetMetaDataInitialization_DescribeQuery() throws SQLException {

    // [columnName, columnType, expectedTypeName, expectedIntegerType, expectedPrecision,
    // expectedScale]
    Object[][] columnData = {
      {"col_int", "int", "INT", Types.INTEGER, 10, 0},
      {"col_string", "string", "STRING", Types.VARCHAR, 255, 0},
      {"col_decimal", "decimal(10,2)", "DECIMAL", Types.DECIMAL, 10, 2},
      {"col_date", "date", "DATE", Types.DATE, 10, 0},
      {"col_timestamp", "timestamp", "TIMESTAMP", Types.TIMESTAMP, 29, 9},
      {"col_timestamp_ntz", "timestamp_ntz", "TIMESTAMP", Types.TIMESTAMP, 29, 9},
      {"col_bool", "boolean", "BOOLEAN", Types.BOOLEAN, 1, 0},
      {"col_binary", "binary", "BINARY", Types.BINARY, 1, 0},
      {"col_struct", "struct<col_int:int,col_string:string>", "STRUCT", Types.STRUCT, 255, 0},
      {"col_array", "array<int>", "ARRAY", Types.ARRAY, 255, 0},
      {"col_map", "map<string,string>", "MAP", Types.VARCHAR, 255, 0},
      {"col_variant", "variant", "VARIANT", Types.VARCHAR, 255, 0},
      {"col_interval", "interval", "INTERVAL", Types.VARCHAR, 255, 0},
      {"col_interval_second", "interval second", "INTERVAL SECOND", Types.VARCHAR, 255, 0},
      {"col_interval_minute", "interval minute", "INTERVAL MINUTE", Types.VARCHAR, 255, 0},
      {"col_interval_hour", "interval hour", "INTERVAL HOUR", Types.VARCHAR, 255, 0},
      {"col_interval_day", "interval day", "INTERVAL DAY", Types.VARCHAR, 255, 0},
      {"col_interval_month", "interval month", "INTERVAL MONTH", Types.VARCHAR, 255, 0},
      {"col_interval_year", "interval year", "INTERVAL YEAR", Types.VARCHAR, 255, 0},
      {
        "col_interval_year_to_month",
        "interval year to month",
        "INTERVAL YEAR TO MONTH",
        Types.VARCHAR,
        255,
        0
      },
      {
        "col_interval_day_to_hour",
        "interval day to hour",
        "INTERVAL DAY TO HOUR",
        Types.VARCHAR,
        255,
        0
      },
      {
        "col_interval_day_to_minute",
        "interval day to minute",
        "INTERVAL DAY TO MINUTE",
        Types.VARCHAR,
        255,
        0
      },
      {
        "col_interval_day_to_second",
        "interval day to second",
        "INTERVAL DAY TO SECOND",
        Types.VARCHAR,
        255,
        0
      },
      {
        "col_interval_hour_to_minute",
        "interval hour to minute",
        "INTERVAL HOUR TO MINUTE",
        Types.VARCHAR,
        255,
        0
      },
      {
        "col_interval_hour_to_second",
        "interval hour to second",
        "INTERVAL HOUR TO SECOND",
        Types.VARCHAR,
        255,
        0
      },
      {
        "col_interval_minute_to_second",
        "interval minute to second",
        "INTERVAL MINUTE TO SECOND",
        Types.VARCHAR,
        255,
        0
      }
    };

    List<String> columnNames =
        Arrays.stream(columnData).map(row -> (String) row[0]).collect(Collectors.toList());

    List<String> columnTypes =
        Arrays.stream(columnData).map(row -> (String) row[1]).collect(Collectors.toList());

    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, columnNames, columnTypes, connectionContext);

    assertEquals(columnNames.size(), metaData.getColumnCount());
    // With Describe query we can't determine total rows
    assertEquals(-1, metaData.getTotalRows());

    for (int i = 0; i < columnData.length; i++) {
      assertEquals(columnData[i][0], metaData.getColumnName(i + 1));
      assertEquals(columnData[i][2], metaData.getColumnTypeName(i + 1));
      assertEquals(columnData[i][3], metaData.getColumnType(i + 1));
      assertEquals(columnData[i][4], metaData.getPrecision(i + 1));
      assertEquals(columnData[i][5], metaData.getScale(i + 1));
    }
  }

  @Test
  public void testColumnsForVolumeOperation() throws SQLException {
    ResultManifest resultManifest = getResultManifest().setIsVolumeOperation(true);
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, false, connectionContext);
    assertEquals(1, metaData.getColumnCount());
    assertEquals(
        DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_COLUMN_NAME, metaData.getColumnName(1));
    assertEquals(10, metaData.getTotalRows());
    assertEquals(
        1,
        metaData.getColumnNameIndex(DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_COLUMN_NAME));
  }

  @Test
  public void testColumnsForVolumeOperationForThrift() throws SQLException {
    TGetResultSetMetadataResp resultManifest = getThriftResultManifest();
    resultManifest.setIsStagingOperationIsSet(true);
    resultManifest.setIsStagingOperation(true);
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(
            THRIFT_STATEMENT_ID, resultManifest, 1, 1, null, connectionContext);
    Assertions.assertEquals(1, metaData.getColumnCount());
    Assertions.assertEquals(
        DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_COLUMN_NAME, metaData.getColumnName(1));
    Assertions.assertEquals(1, metaData.getTotalRows());
    Assertions.assertEquals(
        1,
        metaData.getColumnNameIndex(DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_COLUMN_NAME));
  }

  @Test
  public void testColumnsWithVariantTypeThrift() throws Exception {
    TGetResultSetMetadataResp resultManifest = getThriftResultManifest();
    TColumnDesc columnDesc = new TColumnDesc().setColumnName("testCol");
    TTypeDesc typeDesc = new TTypeDesc();
    TTypeEntry typeEntry = new TTypeEntry();
    TPrimitiveTypeEntry primitiveEntry = new TPrimitiveTypeEntry(TTypeId.STRING_TYPE);
    typeEntry.setPrimitiveEntry(primitiveEntry);
    typeDesc.setTypes(Collections.singletonList(typeEntry));
    columnDesc.setTypeDesc(typeDesc);
    TTableSchema schema = new TTableSchema().setColumns(Collections.singletonList(columnDesc));
    resultManifest.setSchema(schema);
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(
            THRIFT_STATEMENT_ID, resultManifest, 1, 1, List.of(VARIANT), connectionContext);
    assertEquals(1, metaData.getColumnCount());
    assertEquals("testCol", metaData.getColumnName(1));
    assertEquals(1, metaData.getTotalRows());
    assertEquals(1, metaData.getColumnNameIndex("testCol"));
    assertEquals(1, metaData.getColumnNameIndex("TESTCol"));
    assertEquals(Types.OTHER, metaData.getColumnType(1));
    assertEquals("java.lang.String", metaData.getColumnClassName(1));
    assertEquals(VARIANT, metaData.getColumnTypeName(1));
    assertEquals(255, metaData.getPrecision(1));
    assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(1));

    List<String> nullArrowMetadata = Collections.singletonList(null);
    metaData =
        new DatabricksResultSetMetaData(
            THRIFT_STATEMENT_ID, resultManifest, 1, 1, nullArrowMetadata, connectionContext);
    assertEquals(Types.VARCHAR, metaData.getColumnType(1));
  }

  @Test
  public void testThriftColumns() throws SQLException {
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(
            THRIFT_STATEMENT_ID, getThriftResultManifest(), 10, 1, null, connectionContext);
    assertEquals(10, metaData.getTotalRows());
    assertEquals(1, metaData.getColumnCount());
    assertEquals("testCol", metaData.getColumnName(1));
  }

  @Test
  public void testEmptyAndNullThriftColumns() throws SQLException {
    TGetResultSetMetadataResp resultSetMetadataResp = new TGetResultSetMetadataResp();
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(
            THRIFT_STATEMENT_ID, resultSetMetadataResp, 0, 1, null, connectionContext);
    assertEquals(0, metaData.getColumnCount());

    resultSetMetadataResp.setSchema(new TTableSchema());
    assertEquals(0, metaData.getColumnCount());
  }

  @Test
  public void testGetPrecisionAndScaleWithColumnInfo() {
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(
            STATEMENT_ID, getResultManifest(), false, connectionContext);
    ColumnInfo decimalColumnInfo = getColumn("col1", ColumnInfoTypeName.DECIMAL, "decimal");
    decimalColumnInfo.setTypePrecision(10L);
    decimalColumnInfo.setTypeScale(2L);

    int[] precisionAndScale =
        metaData.getPrecisionAndScale(
            decimalColumnInfo, DatabricksTypeUtil.getColumnType(decimalColumnInfo.getTypeName()));
    assertEquals(10, precisionAndScale[0]);
    assertEquals(2, precisionAndScale[1]);

    ColumnInfo stringColumnInfo = getColumn("col2", ColumnInfoTypeName.STRING, "string");
    precisionAndScale =
        metaData.getPrecisionAndScale(
            stringColumnInfo, DatabricksTypeUtil.getColumnType(stringColumnInfo.getTypeName()));
    assertEquals(255, precisionAndScale[0]);
    assertEquals(0, precisionAndScale[1]);
  }

  @Test
  public void testColumnBuilderDefaultMetadata() throws SQLException {
    ResultManifest resultManifest = getResultManifest();
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, false, connectionContext);
    assertEquals(4, metaData.getColumnCount());
    verifyDefaultMetadataProperties(metaData, StatementType.SQL);

    metaData =
        new DatabricksResultSetMetaData(
            STATEMENT_ID,
            List.of("col1", "col2", "col2"),
            List.of("int", "string", "double"),
            List.of(4, 12, 8),
            List.of(0, 0, 0),
            List.of(NULLABLE, NULLABLE, NULLABLE),
            10);
    assertEquals(3, metaData.getColumnCount());
    verifyDefaultMetadataProperties(metaData, StatementType.METADATA);

    TGetResultSetMetadataResp thriftResultManifest = getThriftResultManifest();
    metaData =
        new DatabricksResultSetMetaData(
            STATEMENT_ID, thriftResultManifest, 1, 1, null, connectionContext);
    assertEquals(1, metaData.getColumnCount());
    verifyDefaultMetadataProperties(metaData, StatementType.SQL);
  }

  @Test
  public void testGetPrecisionAndScaleWithColumnInfoWithoutType() {
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(
            THRIFT_STATEMENT_ID, getResultManifest(), false, connectionContext);

    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setTypeName(ColumnInfoTypeName.DECIMAL);
    columnInfo.setTypePrecision(10L);
    columnInfo.setTypeScale(2L);
    int[] precisionAndScale = metaData.getPrecisionAndScale(columnInfo);
    assertEquals(10, precisionAndScale[0]);
    assertEquals(2, precisionAndScale[1]);

    // Test with string type
    columnInfo = new ColumnInfo();
    columnInfo.setTypeName(ColumnInfoTypeName.STRING);

    precisionAndScale = metaData.getPrecisionAndScale(columnInfo);
    assertEquals(255, precisionAndScale[0]);
    assertEquals(0, precisionAndScale[1]);
  }

  @ParameterizedTest
  @MethodSource("thriftResultFormats")
  public void testGetDispositionThrift(TSparkRowSetType resultFormat) {
    TGetResultSetMetadataResp thriftResultManifest = getThriftResultManifest();
    thriftResultManifest.setResultFormat(resultFormat);
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(
            STATEMENT_ID, thriftResultManifest, 1, 1, null, connectionContext);

    if (resultFormat == TSparkRowSetType.URL_BASED_SET) {
      assertTrue(metaData.getIsCloudFetchUsed());
    } else {
      assertFalse(metaData.getIsCloudFetchUsed());
    }
  }

  @Test
  public void testCloudFetchUsedSdk() {
    ResultManifest resultManifest = getResultManifest();

    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, true, connectionContext);
    assertTrue(metaData.getIsCloudFetchUsed());

    metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, false, connectionContext);
    assertFalse(metaData.getIsCloudFetchUsed());
  }

  @Test
  public void testSEAInlineComplexType() throws SQLException {
    ResultManifest resultManifest = new ResultManifest();
    resultManifest.setTotalRowCount(1L);
    ResultSchema schema = new ResultSchema();
    schema.setColumnCount(3L);

    ColumnInfo arrayColumn = getColumn("array_col", ColumnInfoTypeName.ARRAY, "ARRAY<INT>");
    ColumnInfo structColumn =
        getColumn("struct_col", ColumnInfoTypeName.STRUCT, "STRUCT<field1:INT,field2:STRING>");
    ColumnInfo mapColumn = getColumn("map_col", ColumnInfoTypeName.MAP, "MAP<STRING,INT>");

    schema.setColumns(List.of(arrayColumn, structColumn, mapColumn));
    resultManifest.setSchema(schema);

    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, false, connectionContext);

    assertEquals("ARRAY<INT>", metaData.getColumnTypeName(1));
    assertEquals("STRUCT<field1:INT,field2:STRING>", metaData.getColumnTypeName(2));
    assertEquals("MAP<STRING,INT>", metaData.getColumnTypeName(3));

    assertEquals("array_col", metaData.getColumnName(1));
    assertEquals("struct_col", metaData.getColumnName(2));
    assertEquals("map_col", metaData.getColumnName(3));

    assertEquals(Types.ARRAY, metaData.getColumnType(1));
    assertEquals(Types.STRUCT, metaData.getColumnType(2));
    assertEquals(Types.VARCHAR, metaData.getColumnType(3));
  }

  @Test
  public void testThriftComplexTypeWithArrowMetadata() throws SQLException {
    // Create a Thrift result manifest with ARRAY, MAP, and STRUCT columns
    TGetResultSetMetadataResp resultManifest = new TGetResultSetMetadataResp();

    // Create ARRAY column descriptor (TTypeDesc only has primitive ARRAY_TYPE)
    TColumnDesc arrayColumn = new TColumnDesc().setColumnName("array_col").setPosition(1);
    TTypeDesc arrayTypeDesc = new TTypeDesc();
    TTypeEntry arrayTypeEntry = new TTypeEntry();
    TPrimitiveTypeEntry arrayPrimitiveEntry = new TPrimitiveTypeEntry(TTypeId.ARRAY_TYPE);
    arrayTypeEntry.setPrimitiveEntry(arrayPrimitiveEntry);
    arrayTypeDesc.setTypes(Collections.singletonList(arrayTypeEntry));
    arrayColumn.setTypeDesc(arrayTypeDesc);

    // Create MAP column descriptor (TTypeDesc only has primitive MAP_TYPE)
    TColumnDesc mapColumn = new TColumnDesc().setColumnName("map_col").setPosition(2);
    TTypeDesc mapTypeDesc = new TTypeDesc();
    TTypeEntry mapTypeEntry = new TTypeEntry();
    TPrimitiveTypeEntry mapPrimitiveEntry = new TPrimitiveTypeEntry(TTypeId.MAP_TYPE);
    mapTypeEntry.setPrimitiveEntry(mapPrimitiveEntry);
    mapTypeDesc.setTypes(Collections.singletonList(mapTypeEntry));
    mapColumn.setTypeDesc(mapTypeDesc);

    // Create STRUCT column descriptor (TTypeDesc only has primitive STRUCT_TYPE)
    TColumnDesc structColumn = new TColumnDesc().setColumnName("struct_col").setPosition(3);
    TTypeDesc structTypeDesc = new TTypeDesc();
    TTypeEntry structTypeEntry = new TTypeEntry();
    TPrimitiveTypeEntry structPrimitiveEntry = new TPrimitiveTypeEntry(TTypeId.STRUCT_TYPE);
    structTypeEntry.setPrimitiveEntry(structPrimitiveEntry);
    structTypeDesc.setTypes(Collections.singletonList(structTypeEntry));
    structColumn.setTypeDesc(structTypeDesc);

    TTableSchema schema =
        new TTableSchema().setColumns(List.of(arrayColumn, mapColumn, structColumn));
    resultManifest.setSchema(schema);

    // Arrow metadata contains full type information
    List<String> arrowMetadata =
        List.of("ARRAY<INT>", "MAP<STRING,INT>", "STRUCT<field1:INT,field2:STRING>");

    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(
            THRIFT_STATEMENT_ID, resultManifest, 1, 1, arrowMetadata, connectionContext);

    // Verify that arrowMetadata is used for type names (not the primitive types from TTypeDesc)
    assertEquals("ARRAY<INT>", metaData.getColumnTypeName(1));
    assertEquals("MAP<STRING,INT>", metaData.getColumnTypeName(2));
    assertEquals("STRUCT<field1:INT,field2:STRING>", metaData.getColumnTypeName(3));

    assertEquals("array_col", metaData.getColumnName(1));
    assertEquals("map_col", metaData.getColumnName(2));
    assertEquals("struct_col", metaData.getColumnName(3));

    assertEquals(Types.ARRAY, metaData.getColumnType(1));
    assertEquals(Types.VARCHAR, metaData.getColumnType(2));
    assertEquals(Types.STRUCT, metaData.getColumnType(3));
  }

  @Test
  public void testThriftComplexTypeWithoutArrowMetadata() throws SQLException {
    // Test fallback behavior when arrowMetadata is not available
    TGetResultSetMetadataResp resultManifest = new TGetResultSetMetadataResp();

    TColumnDesc arrayColumn = new TColumnDesc().setColumnName("array_col").setPosition(1);
    TTypeDesc arrayTypeDesc = new TTypeDesc();
    TTypeEntry arrayTypeEntry = new TTypeEntry();
    TPrimitiveTypeEntry arrayPrimitiveEntry = new TPrimitiveTypeEntry(TTypeId.ARRAY_TYPE);
    arrayTypeEntry.setPrimitiveEntry(arrayPrimitiveEntry);
    arrayTypeDesc.setTypes(Collections.singletonList(arrayTypeEntry));
    arrayColumn.setTypeDesc(arrayTypeDesc);

    TTableSchema schema = new TTableSchema().setColumns(Collections.singletonList(arrayColumn));
    resultManifest.setSchema(schema);

    // No arrow metadata - should fall back to TTypeDesc
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(
            THRIFT_STATEMENT_ID, resultManifest, 1, 1, null, connectionContext);

    // Without arrowMetadata, falls back to primitive type name from TTypeDesc
    assertEquals("ARRAY", metaData.getColumnTypeName(1));
    assertEquals("array_col", metaData.getColumnName(1));
    assertEquals(Types.ARRAY, metaData.getColumnType(1));
  }

  private void verifyDefaultMetadataProperties(
      DatabricksResultSetMetaData metaData, StatementType type) throws SQLException {
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      // verify metadata properties default value
      assertFalse(metaData.isAutoIncrement(i));
      assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(i));
      assertFalse(metaData.isDefinitelyWritable(i));
      assertEquals(type == StatementType.METADATA ? "" : null, metaData.getSchemaName(i));
      assertEquals(type == StatementType.METADATA ? "" : null, metaData.getTableName(i));
      assertEquals("", metaData.getCatalogName(i));
      assertFalse(metaData.isCurrency(i));
      assertEquals(0, metaData.getScale(i));
      assertFalse(metaData.isCaseSensitive(i));
    }
  }

  @Test
  public void testGeospatialTypesConvertedToStringWhenFlagDisabled() throws SQLException {
    // Mock connection context with geospatial support disabled
    when(connectionContext.isGeoSpatialSupportEnabled()).thenReturn(false);

    ResultManifest resultManifest = new ResultManifest();
    resultManifest.setTotalRowCount(1L);
    ResultSchema schema = new ResultSchema();
    schema.setColumnCount(2L);

    ColumnInfo geometryColumn = getColumn("geom_col", ColumnInfoTypeName.GEOMETRY, "GEOMETRY");
    ColumnInfo geographyColumn = getColumn("geog_col", ColumnInfoTypeName.GEOGRAPHY, "GEOGRAPHY");

    schema.setColumns(List.of(geometryColumn, geographyColumn));
    resultManifest.setSchema(schema);

    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, false, connectionContext);

    // Verify GEOMETRY column is converted to STRING
    assertEquals("geom_col", metaData.getColumnName(1));
    assertEquals("STRING", metaData.getColumnTypeName(1));
    assertEquals(Types.VARCHAR, metaData.getColumnType(1));
    assertEquals("java.lang.String", metaData.getColumnClassName(1));

    // Verify GEOGRAPHY column is converted to STRING
    assertEquals("geog_col", metaData.getColumnName(2));
    assertEquals("STRING", metaData.getColumnTypeName(2));
    assertEquals(Types.VARCHAR, metaData.getColumnType(2));
    assertEquals("java.lang.String", metaData.getColumnClassName(2));
  }

  @Test
  public void testGeospatialTypesRetainedWhenFlagEnabled() throws SQLException {
    // Mock connection context with geospatial support enabled
    when(connectionContext.isGeoSpatialSupportEnabled()).thenReturn(true);

    ResultManifest resultManifest = new ResultManifest();
    resultManifest.setTotalRowCount(1L);
    ResultSchema schema = new ResultSchema();
    schema.setColumnCount(2L);

    ColumnInfo geometryColumn = getColumn("geom_col", ColumnInfoTypeName.GEOMETRY, "GEOMETRY");
    ColumnInfo geographyColumn = getColumn("geog_col", ColumnInfoTypeName.GEOGRAPHY, "GEOGRAPHY");

    schema.setColumns(List.of(geometryColumn, geographyColumn));
    resultManifest.setSchema(schema);

    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, false, connectionContext);

    // Verify GEOMETRY column retains its type
    assertEquals("geom_col", metaData.getColumnName(1));
    assertEquals("GEOMETRY", metaData.getColumnTypeName(1));
    assertEquals(Types.OTHER, metaData.getColumnType(1));
    assertEquals(
        DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.GEOMETRY),
        metaData.getColumnClassName(1));

    // Verify GEOGRAPHY column retains its type
    assertEquals("geog_col", metaData.getColumnName(2));
    assertEquals("GEOGRAPHY", metaData.getColumnTypeName(2));
    assertEquals(Types.OTHER, metaData.getColumnType(2));
    assertEquals(
        DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.GEOGRAPHY),
        metaData.getColumnClassName(2));
  }

  @Test
  public void testMixedColumnsWithGeospatialFlagDisabled() throws SQLException {
    // Mock connection context with geospatial support disabled
    when(connectionContext.isGeoSpatialSupportEnabled()).thenReturn(false);

    ResultManifest resultManifest = new ResultManifest();
    resultManifest.setTotalRowCount(1L);
    ResultSchema schema = new ResultSchema();
    schema.setColumnCount(4L);

    ColumnInfo intColumn = getColumn("id", ColumnInfoTypeName.INT, "INT");
    ColumnInfo stringColumn = getColumn("name", ColumnInfoTypeName.STRING, "STRING");
    ColumnInfo geometryColumn = getColumn("location", ColumnInfoTypeName.GEOMETRY, "GEOMETRY");
    ColumnInfo geographyColumn = getColumn("region", ColumnInfoTypeName.GEOGRAPHY, "GEOGRAPHY");

    schema.setColumns(List.of(intColumn, stringColumn, geometryColumn, geographyColumn));
    resultManifest.setSchema(schema);

    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, false, connectionContext);

    // Verify non-geospatial columns are not affected
    assertEquals("id", metaData.getColumnName(1));
    assertEquals("INT", metaData.getColumnTypeName(1));
    assertEquals(Types.INTEGER, metaData.getColumnType(1));

    assertEquals("name", metaData.getColumnName(2));
    assertEquals("STRING", metaData.getColumnTypeName(2));
    assertEquals(Types.VARCHAR, metaData.getColumnType(2));

    // Verify geospatial columns are converted to STRING
    assertEquals("location", metaData.getColumnName(3));
    assertEquals("STRING", metaData.getColumnTypeName(3));
    assertEquals(Types.VARCHAR, metaData.getColumnType(3));

    assertEquals("region", metaData.getColumnName(4));
    assertEquals("STRING", metaData.getColumnTypeName(4));
    assertEquals(Types.VARCHAR, metaData.getColumnType(4));
  }

  @Test
  public void testJsonArrayFormatMetadataWithGeospatialFlagDisabled() throws SQLException {
    // This test explicitly verifies that metadata returns STRING for geospatial columns
    // when using JSON_ARRAY format (SQL Exec API with JSON format)

    // Mock connection context with geospatial support disabled
    when(connectionContext.isGeoSpatialSupportEnabled()).thenReturn(false);

    ResultManifest resultManifest = new ResultManifest();
    resultManifest.setTotalRowCount(2L);
    // Explicitly set format to JSON_ARRAY to test SQL Exec API JSON format
    resultManifest.setFormat(com.databricks.sdk.service.sql.Format.JSON_ARRAY);

    ResultSchema schema = new ResultSchema();
    schema.setColumnCount(3L);

    ColumnInfo idColumn = getColumn("store_id", ColumnInfoTypeName.INT, "INT");
    ColumnInfo geometryColumn =
        getColumn("store_location", ColumnInfoTypeName.GEOMETRY, "GEOMETRY");
    ColumnInfo geographyColumn =
        getColumn("delivery_region", ColumnInfoTypeName.GEOGRAPHY, "GEOGRAPHY");

    schema.setColumns(List.of(idColumn, geometryColumn, geographyColumn));
    resultManifest.setSchema(schema);

    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, false, connectionContext);

    // Verify non-geospatial column is not affected
    assertEquals("store_id", metaData.getColumnName(1));
    assertEquals("INT", metaData.getColumnTypeName(1));
    assertEquals(Types.INTEGER, metaData.getColumnType(1));
    assertEquals("java.lang.Integer", metaData.getColumnClassName(1));

    // Verify GEOMETRY column metadata returns STRING (not GEOMETRY)
    assertEquals("store_location", metaData.getColumnName(2));
    assertEquals("STRING", metaData.getColumnTypeName(2));
    assertEquals(Types.VARCHAR, metaData.getColumnType(2));
    assertEquals("java.lang.String", metaData.getColumnClassName(2));

    // Verify GEOGRAPHY column metadata returns STRING (not GEOGRAPHY)
    assertEquals("delivery_region", metaData.getColumnName(3));
    assertEquals("STRING", metaData.getColumnTypeName(3));
    assertEquals(Types.VARCHAR, metaData.getColumnType(3));
    assertEquals("java.lang.String", metaData.getColumnClassName(3));
  }

  @Test
  public void testJsonArrayWithComplexTypesEnabledButGeospatialDisabled() throws SQLException {
    // This test validates the important scenario where EnableComplexDatatypeSupport=1
    // but EnableGeoSpatialSupport=0 (disabled). This simulates real-world usage where
    // users want complex types (ARRAY, MAP, STRUCT) but want geospatial data as strings.
    //
    // Expected behavior:
    // - GEOMETRY/GEOGRAPHY column types should report as STRING in metadata
    // - ARRAY/MAP column types should report correctly (not affected)

    // Explicitly enable complex data type support (EnableComplexDatatypeSupport=1)
    when(connectionContext.isComplexDatatypeSupportEnabled()).thenReturn(true);
    // Disable geospatial support (EnableGeoSpatialSupport=0)
    when(connectionContext.isGeoSpatialSupportEnabled()).thenReturn(false);

    ResultManifest resultManifest = new ResultManifest();
    resultManifest.setTotalRowCount(2L);
    resultManifest.setFormat(com.databricks.sdk.service.sql.Format.JSON_ARRAY);

    ResultSchema schema = new ResultSchema();
    schema.setColumnCount(5L);

    ColumnInfo idColumn = getColumn("id", ColumnInfoTypeName.INT, "INT");
    ColumnInfo geometryColumn = getColumn("location", ColumnInfoTypeName.GEOMETRY, "GEOMETRY");
    ColumnInfo geographyColumn = getColumn("region", ColumnInfoTypeName.GEOGRAPHY, "GEOGRAPHY");
    // Complex types that should work normally when complex support is enabled
    ColumnInfo arrayColumn = getColumn("tags", ColumnInfoTypeName.ARRAY, "ARRAY<STRING>");
    ColumnInfo mapColumn = getColumn("metadata", ColumnInfoTypeName.MAP, "MAP<STRING,STRING>");

    schema.setColumns(List.of(idColumn, geometryColumn, geographyColumn, arrayColumn, mapColumn));
    resultManifest.setSchema(schema);

    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, false, connectionContext);

    // Verify regular column is not affected
    assertEquals("id", metaData.getColumnName(1));
    assertEquals("INT", metaData.getColumnTypeName(1));
    assertEquals(Types.INTEGER, metaData.getColumnType(1));
    assertEquals("java.lang.Integer", metaData.getColumnClassName(1));

    // === Verify GEOSPATIAL columns return STRING type ===
    // GEOMETRY column should report as STRING
    assertEquals("location", metaData.getColumnName(2));
    assertEquals("STRING", metaData.getColumnTypeName(2));
    assertEquals(Types.VARCHAR, metaData.getColumnType(2));
    assertEquals("java.lang.String", metaData.getColumnClassName(2));

    // GEOGRAPHY column should report as STRING
    assertEquals("region", metaData.getColumnName(3));
    assertEquals("STRING", metaData.getColumnTypeName(3));
    assertEquals(Types.VARCHAR, metaData.getColumnType(3));
    assertEquals("java.lang.String", metaData.getColumnClassName(3));

    // === Verify COMPLEX TYPE columns are NOT affected ===
    // ARRAY column should report correctly (not converted to STRING)
    assertEquals("tags", metaData.getColumnName(4));
    assertEquals("ARRAY<STRING>", metaData.getColumnTypeName(4));
    assertEquals(Types.ARRAY, metaData.getColumnType(4));
    assertEquals("java.sql.Array", metaData.getColumnClassName(4));

    // MAP column should report correctly (not converted to STRING)
    assertEquals("metadata", metaData.getColumnName(5));
    assertEquals("MAP<STRING,STRING>", metaData.getColumnTypeName(5));
    assertEquals(Types.VARCHAR, metaData.getColumnType(5)); // MAP uses VARCHAR type
    assertEquals("java.util.Map", metaData.getColumnClassName(5));
  }

  @Test
  public void testThriftGeometryColumnConvertedToStringWhenFlagDisabled() throws SQLException {
    // Mock connection context with geospatial support disabled
    when(connectionContext.isGeoSpatialSupportEnabled()).thenReturn(false);

    TGetResultSetMetadataResp resultManifest = new TGetResultSetMetadataResp();
    TColumnDesc columnDesc = new TColumnDesc().setColumnName("location");
    TTypeDesc typeDesc = new TTypeDesc();
    TTypeEntry typeEntry = new TTypeEntry();
    TPrimitiveTypeEntry primitiveEntry = new TPrimitiveTypeEntry(TTypeId.STRING_TYPE);
    typeEntry.setPrimitiveEntry(primitiveEntry);
    typeDesc.setTypes(Collections.singletonList(typeEntry));
    columnDesc.setTypeDesc(typeDesc);
    TTableSchema schema = new TTableSchema().setColumns(Collections.singletonList(columnDesc));
    resultManifest.setSchema(schema);

    // Arrow metadata indicates this is a GEOMETRY column
    List<String> arrowMetadata = List.of("GEOMETRY");

    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(
            THRIFT_STATEMENT_ID, resultManifest, 1, 1, arrowMetadata, connectionContext);

    // Verify GEOMETRY column is converted to STRING
    assertEquals("location", metaData.getColumnName(1));
    assertEquals("STRING", metaData.getColumnTypeName(1));
    assertEquals(Types.VARCHAR, metaData.getColumnType(1));
    assertEquals("java.lang.String", metaData.getColumnClassName(1));
  }

  @Test
  public void testThriftGeographyColumnConvertedToStringWhenFlagDisabled() throws SQLException {
    // Mock connection context with geospatial support disabled
    when(connectionContext.isGeoSpatialSupportEnabled()).thenReturn(false);

    TGetResultSetMetadataResp resultManifest = new TGetResultSetMetadataResp();
    TColumnDesc columnDesc = new TColumnDesc().setColumnName("region");
    TTypeDesc typeDesc = new TTypeDesc();
    TTypeEntry typeEntry = new TTypeEntry();
    TPrimitiveTypeEntry primitiveEntry = new TPrimitiveTypeEntry(TTypeId.STRING_TYPE);
    typeEntry.setPrimitiveEntry(primitiveEntry);
    typeDesc.setTypes(Collections.singletonList(typeEntry));
    columnDesc.setTypeDesc(typeDesc);
    TTableSchema schema = new TTableSchema().setColumns(Collections.singletonList(columnDesc));
    resultManifest.setSchema(schema);

    // Arrow metadata indicates this is a GEOGRAPHY column
    List<String> arrowMetadata = List.of("GEOGRAPHY");

    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(
            THRIFT_STATEMENT_ID, resultManifest, 1, 1, arrowMetadata, connectionContext);

    // Verify GEOGRAPHY column is converted to STRING
    assertEquals("region", metaData.getColumnName(1));
    assertEquals("STRING", metaData.getColumnTypeName(1));
    assertEquals(Types.VARCHAR, metaData.getColumnType(1));
    assertEquals("java.lang.String", metaData.getColumnClassName(1));
  }

  @Test
  public void testThriftGeospatialColumnsRetainedWhenFlagEnabled() throws SQLException {
    // Mock connection context with geospatial support enabled
    when(connectionContext.isGeoSpatialSupportEnabled()).thenReturn(true);

    TGetResultSetMetadataResp resultManifest = new TGetResultSetMetadataResp();

    // Create two columns - one GEOMETRY, one GEOGRAPHY
    TColumnDesc geometryColumn = new TColumnDesc().setColumnName("location").setPosition(1);
    TTypeDesc geometryTypeDesc = new TTypeDesc();
    TTypeEntry geometryTypeEntry = new TTypeEntry();
    TPrimitiveTypeEntry geometryPrimitiveEntry = new TPrimitiveTypeEntry(TTypeId.STRING_TYPE);
    geometryTypeEntry.setPrimitiveEntry(geometryPrimitiveEntry);
    geometryTypeDesc.setTypes(Collections.singletonList(geometryTypeEntry));
    geometryColumn.setTypeDesc(geometryTypeDesc);

    TColumnDesc geographyColumn = new TColumnDesc().setColumnName("region").setPosition(2);
    TTypeDesc geographyTypeDesc = new TTypeDesc();
    TTypeEntry geographyTypeEntry = new TTypeEntry();
    TPrimitiveTypeEntry geographyPrimitiveEntry = new TPrimitiveTypeEntry(TTypeId.STRING_TYPE);
    geographyTypeEntry.setPrimitiveEntry(geographyPrimitiveEntry);
    geographyTypeDesc.setTypes(Collections.singletonList(geographyTypeEntry));
    geographyColumn.setTypeDesc(geographyTypeDesc);

    TTableSchema schema = new TTableSchema().setColumns(List.of(geometryColumn, geographyColumn));
    resultManifest.setSchema(schema);

    // Arrow metadata indicates geospatial types
    List<String> arrowMetadata = List.of("GEOMETRY", "GEOGRAPHY");

    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(
            THRIFT_STATEMENT_ID, resultManifest, 1, 1, arrowMetadata, connectionContext);

    // Verify GEOMETRY column retains its type
    assertEquals("location", metaData.getColumnName(1));
    assertEquals("GEOMETRY", metaData.getColumnTypeName(1));
    assertEquals(Types.OTHER, metaData.getColumnType(1));

    // Verify GEOGRAPHY column retains its type
    assertEquals("region", metaData.getColumnName(2));
    assertEquals("GEOGRAPHY", metaData.getColumnTypeName(2));
    assertEquals(Types.OTHER, metaData.getColumnType(2));
  }
}
