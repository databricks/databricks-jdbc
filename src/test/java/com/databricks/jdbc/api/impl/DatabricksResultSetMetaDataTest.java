package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.Nullable.NULLABLE;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.getTypeFromTypeDesc;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.VARIANT;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.service.sql.*;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class DatabricksResultSetMetaDataTest {
  private static final StatementId STATEMENT_ID = new StatementId("statementId");
  private static final StatementId THRIFT_STATEMENT_ID =
      StatementId.deserialize(
          "01efc77c-7c8b-1a8e-9ecb-a9a6e6aa050a|338d529d-8272-46eb-8482-cb419466839d");

  static Stream<TSparkRowSetType> thriftResultFormats() {
    return Stream.of(
        TSparkRowSetType.ARROW_BASED_SET,
        TSparkRowSetType.COLUMN_BASED_SET,
        TSparkRowSetType.ROW_BASED_SET,
        TSparkRowSetType.URL_BASED_SET);
  }

  static Stream<Format> sdkResultFormats() {
    return Stream.of(Format.ARROW_STREAM, Format.CSV, Format.JSON_ARRAY);
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
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest);
    assertEquals(4, metaData.getColumnCount());
    assertEquals("col1", metaData.getColumnName(1));
    assertEquals("col2", metaData.getColumnName(2));
    assertEquals("col2", metaData.getColumnName(3));
    assertEquals("col5", metaData.getColumnName(4));
    assertEquals(10, metaData.getTotalRows());
    assertEquals(2, metaData.getColumnNameIndex("col2"));

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
  public void testColumnsForVolumeOperation() throws SQLException {
    ResultManifest resultManifest = getResultManifest().setIsVolumeOperation(true);
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest);
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
        new DatabricksResultSetMetaData(THRIFT_STATEMENT_ID, resultManifest, 1, 1, null);
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
            THRIFT_STATEMENT_ID, resultManifest, 1, 1, List.of(VARIANT));
    assertEquals(1, metaData.getColumnCount());
    assertEquals("testCol", metaData.getColumnName(1));
    assertEquals(1, metaData.getTotalRows());
    assertEquals(1, metaData.getColumnNameIndex("testCol"));
    assertEquals(Types.OTHER, metaData.getColumnType(1));
    assertEquals("java.lang.String", metaData.getColumnClassName(1));
    assertEquals(VARIANT, metaData.getColumnTypeName(1));
    assertEquals(255, metaData.getPrecision(1));
    assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(1));
  }

  @Test
  public void testThriftColumns() throws SQLException {
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(
            THRIFT_STATEMENT_ID, getThriftResultManifest(), 10, 1, null);
    assertEquals(10, metaData.getTotalRows());
    assertEquals(1, metaData.getColumnCount());
    assertEquals("testCol", metaData.getColumnName(1));
  }

  @Test
  public void testEmptyAndNullThriftColumns() throws SQLException {
    TGetResultSetMetadataResp resultSetMetadataResp = new TGetResultSetMetadataResp();
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(THRIFT_STATEMENT_ID, resultSetMetadataResp, 0, 1, null);
    assertEquals(0, metaData.getColumnCount());

    resultSetMetadataResp.setSchema(new TTableSchema());
    assertEquals(0, metaData.getColumnCount());
  }

  @Test
  public void testGetScaleAndPrecisionWithColumnInfo() throws SQLException {
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, getResultManifest());
    ColumnInfo decimalColumnInfo = getColumn("col1", ColumnInfoTypeName.DECIMAL, "decimal");
    decimalColumnInfo.setTypePrecision(10L);
    decimalColumnInfo.setTypeScale(2L);

    int[] scaleAndPrecision =
        metaData.getScaleAndPrecision(
            decimalColumnInfo, DatabricksTypeUtil.getColumnType(decimalColumnInfo.getTypeName()));
    assertEquals(10, scaleAndPrecision[0]);
    assertEquals(2, scaleAndPrecision[1]);

    ColumnInfo stringColumnInfo = getColumn("col2", ColumnInfoTypeName.STRING, "string");
    scaleAndPrecision =
        metaData.getScaleAndPrecision(
            stringColumnInfo, DatabricksTypeUtil.getColumnType(stringColumnInfo.getTypeName()));
    assertEquals(255, scaleAndPrecision[0]);
    assertEquals(0, scaleAndPrecision[1]);
  }

  @Test
  public void testColumnBuilderDefaultMetadata() throws SQLException {
    ResultManifest resultManifest = getResultManifest();
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest);
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
    metaData = new DatabricksResultSetMetaData(STATEMENT_ID, thriftResultManifest, 1, 1, null);
    assertEquals(1, metaData.getColumnCount());
    verifyDefaultMetadataProperties(metaData, StatementType.SQL);
  }

  @Test
  public void testGetScaleAndPrecisionWithTColumnDesc() {
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(THRIFT_STATEMENT_ID, getResultManifest());

    TColumnDesc columnInfo = new TColumnDesc();
    TTypeDesc typeDesc = new TTypeDesc();
    TTypeEntry typeEntry = new TTypeEntry();
    TPrimitiveTypeEntry primitiveEntry = new TPrimitiveTypeEntry(TTypeId.DECIMAL_TYPE);
    Map<String, TTypeQualifierValue> qualifiers = new HashMap<>();
    TTypeQualifierValue scaleValue = new TTypeQualifierValue();
    scaleValue.setI32Value(2);
    TTypeQualifierValue precisionValue = new TTypeQualifierValue();
    precisionValue.setI32Value(10);
    qualifiers.put("scale", scaleValue);
    qualifiers.put("precision", precisionValue);
    TTypeQualifiers typeQualifiers = new TTypeQualifiers().setQualifiers(qualifiers);
    primitiveEntry.setTypeQualifiers(typeQualifiers);
    typeEntry.setPrimitiveEntry(primitiveEntry);
    typeDesc.setTypes(Collections.singletonList(typeEntry));
    columnInfo.setTypeDesc(typeDesc);

    int[] scaleAndPrecision =
        metaData.getScaleAndPrecision(
            columnInfo,
            DatabricksTypeUtil.getColumnType(getTypeFromTypeDesc(columnInfo.getTypeDesc())));
    assertEquals(10, scaleAndPrecision[0]);
    assertEquals(2, scaleAndPrecision[1]);

    // Test with string type
    columnInfo = new TColumnDesc();
    typeDesc = new TTypeDesc();
    typeEntry = new TTypeEntry();
    primitiveEntry = new TPrimitiveTypeEntry(TTypeId.STRING_TYPE);
    typeEntry.setPrimitiveEntry(primitiveEntry);
    typeDesc.setTypes(Collections.singletonList(typeEntry));
    columnInfo.setTypeDesc(typeDesc);

    scaleAndPrecision =
        metaData.getScaleAndPrecision(
            columnInfo,
            DatabricksTypeUtil.getColumnType(getTypeFromTypeDesc(columnInfo.getTypeDesc())));
    assertEquals(255, scaleAndPrecision[0]);
    assertEquals(0, scaleAndPrecision[1]);
  }

  @ParameterizedTest
  @MethodSource("thriftResultFormats")
  public void testGetDispositionThrift(TSparkRowSetType resultFormat) {
    TGetResultSetMetadataResp thriftResultManifest = getThriftResultManifest();
    thriftResultManifest.setResultFormat(resultFormat);
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, thriftResultManifest, 1, 1, null);

    if (resultFormat == TSparkRowSetType.URL_BASED_SET) {
      assertTrue(metaData.getIsCloudFetchUsed());
    } else {
      assertFalse(metaData.getIsCloudFetchUsed());
    }
  }

  @ParameterizedTest
  @MethodSource("sdkResultFormats")
  public void testDispositionSdk(Format format) {
    ResultManifest resultManifest = getResultManifest();
    resultManifest.setFormat(format);
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest);

    if (format == Format.ARROW_STREAM) {
      assertTrue(metaData.getIsCloudFetchUsed());
    } else {
      assertFalse(metaData.getIsCloudFetchUsed());
    }
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
}
