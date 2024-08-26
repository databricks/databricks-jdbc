package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.util.DatabricksThriftUtil.getTypeFromTypeDesc;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.dbclient.impl.thrift.generated.*;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.ResultSchema;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DatabricksResultSetMetaDataTest {
  private static final String STATEMENT_ID = "statementId";

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
            10);
    assertEquals(3, metaData.getColumnCount());
    assertEquals("col1", metaData.getColumnName(1));
    assertEquals("col2", metaData.getColumnName(2));
    assertEquals("col2", metaData.getColumnName(3));
    assertEquals(10, metaData.getTotalRows());
    assertEquals(2, metaData.getColumnNameIndex("col2"));
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
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, 1, 1);
    Assertions.assertEquals(1, metaData.getColumnCount());
    Assertions.assertEquals(
        DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_COLUMN_NAME, metaData.getColumnName(1));
    Assertions.assertEquals(1, metaData.getTotalRows());
    Assertions.assertEquals(
        1,
        metaData.getColumnNameIndex(DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_COLUMN_NAME));
  }

  @Test
  public void testThriftColumns() throws SQLException {
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, getThriftResultManifest(), 10, 1);
    assertEquals(10, metaData.getTotalRows());
    assertEquals(1, metaData.getColumnCount());
    assertEquals("testCol", metaData.getColumnName(1));
  }

  @Test
  public void testEmptyAndNullThriftColumns() throws SQLException {
    TGetResultSetMetadataResp resultSetMetadataResp = new TGetResultSetMetadataResp();
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultSetMetadataResp, 0, 1);
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
        metaData.getScaleAndPrecision(decimalColumnInfo, decimalColumnInfo.getTypeName());
    assertEquals(10, scaleAndPrecision[0]);
    assertEquals(2, scaleAndPrecision[1]);

    ColumnInfo stringColumnInfo = getColumn("col2", ColumnInfoTypeName.STRING, "string");
    scaleAndPrecision =
        metaData.getScaleAndPrecision(stringColumnInfo, stringColumnInfo.getTypeName());
    assertEquals(255, scaleAndPrecision[0]);
    assertEquals(0, scaleAndPrecision[1]);
  }

  @Test
  public void testGetScaleAndPrecisionWithTColumnDesc() {
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, getResultManifest());

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
        metaData.getScaleAndPrecision(columnInfo, getTypeFromTypeDesc(columnInfo.getTypeDesc()));
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
        metaData.getScaleAndPrecision(columnInfo, getTypeFromTypeDesc(columnInfo.getTypeDesc()));
    assertEquals(255, scaleAndPrecision[0]);
    assertEquals(0, scaleAndPrecision[1]);
  }
}
