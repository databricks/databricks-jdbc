package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import org.junit.jupiter.api.Test;

class DatabricksTypeUtilTest {

  @Test
  void testGetColumnType() {
    assertEquals(Types.TINYINT, DatabricksTypeUtil.getColumnType(ColumnInfoTypeName.BYTE));
    assertEquals(Types.SMALLINT, DatabricksTypeUtil.getColumnType(ColumnInfoTypeName.SHORT));
    assertEquals(Types.BIGINT, DatabricksTypeUtil.getColumnType(ColumnInfoTypeName.LONG));
    assertEquals(Types.FLOAT, DatabricksTypeUtil.getColumnType(ColumnInfoTypeName.FLOAT));
    assertEquals(Types.DECIMAL, DatabricksTypeUtil.getColumnType(ColumnInfoTypeName.DECIMAL));
    assertEquals(Types.BINARY, DatabricksTypeUtil.getColumnType(ColumnInfoTypeName.BINARY));
    assertEquals(Types.BOOLEAN, DatabricksTypeUtil.getColumnType(ColumnInfoTypeName.BOOLEAN));
    assertEquals(Types.CHAR, DatabricksTypeUtil.getColumnType(ColumnInfoTypeName.CHAR));
    assertEquals(Types.TIMESTAMP, DatabricksTypeUtil.getColumnType(ColumnInfoTypeName.TIMESTAMP));
    assertEquals(Types.DATE, DatabricksTypeUtil.getColumnType(ColumnInfoTypeName.DATE));
    assertEquals(Types.STRUCT, DatabricksTypeUtil.getColumnType(ColumnInfoTypeName.STRUCT));
    assertEquals(Types.ARRAY, DatabricksTypeUtil.getColumnType(ColumnInfoTypeName.ARRAY));
    assertEquals(Types.NULL, DatabricksTypeUtil.getColumnType(ColumnInfoTypeName.NULL));
    assertEquals(
        Types.OTHER, DatabricksTypeUtil.getColumnType(ColumnInfoTypeName.USER_DEFINED_TYPE));
  }

  @Test
  void testGetColumnTypeClassName() {
    assertEquals(
        "java.lang.Long", DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.LONG));
    assertEquals(
        "java.math.BigDecimal",
        DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.DECIMAL));
    assertEquals("[B", DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.BINARY));
    assertEquals(
        "java.sql.Date", DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.DATE));
    assertEquals(
        "java.sql.Struct", DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.STRUCT));
    assertEquals(
        "java.sql.Array", DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.ARRAY));
    assertEquals(
        "java.util.Map", DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.MAP));
    assertEquals("null", DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.NULL));
    assertEquals(
        "java.sql.Timestamp",
        DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.TIMESTAMP));
  }

  @Test
  void testGetDisplaySize() {
    assertEquals(24, DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.FLOAT, 0));
    assertEquals(29, DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.TIMESTAMP, 0));
    assertEquals(1, DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.CHAR, 1));
    assertEquals(4, DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.NULL, 1));
    assertEquals(10, DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.DATE, 1));
  }

  @Test
  void testGetPrecision() {
    assertEquals(15, DatabricksTypeUtil.getPrecision(ColumnInfoTypeName.DOUBLE));
    assertEquals(19, DatabricksTypeUtil.getPrecision(ColumnInfoTypeName.LONG));
    assertEquals(1, DatabricksTypeUtil.getPrecision(ColumnInfoTypeName.BOOLEAN));
    assertEquals(7, DatabricksTypeUtil.getPrecision(ColumnInfoTypeName.FLOAT));
    assertEquals(29, DatabricksTypeUtil.getPrecision(ColumnInfoTypeName.TIMESTAMP));
    assertEquals(255, DatabricksTypeUtil.getPrecision(ColumnInfoTypeName.STRUCT));
    assertEquals(255, DatabricksTypeUtil.getPrecision(ColumnInfoTypeName.INTERVAL));
  }

  @Test
  void testIsSigned() {
    assertTrue(DatabricksTypeUtil.isSigned(ColumnInfoTypeName.INT));
    assertFalse(DatabricksTypeUtil.isSigned(ColumnInfoTypeName.BOOLEAN));
  }

  @Test
  void testGetDatabricksTypeFromSQLType() {
    assertEquals(
        DatabricksTypeUtil.INT, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.INTEGER));
    assertEquals(
        DatabricksTypeUtil.STRING, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.VARCHAR));
    assertEquals(
        DatabricksTypeUtil.STRING,
        DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.LONGVARCHAR));
    assertEquals(
        DatabricksTypeUtil.STRING, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.NVARCHAR));
    assertEquals(
        DatabricksTypeUtil.STRING,
        DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.LONGNVARCHAR));
    assertEquals(
        DatabricksTypeUtil.ARRAY, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.ARRAY));
    assertEquals(
        DatabricksTypeUtil.BIGINT, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.BIGINT));
    assertEquals(
        DatabricksTypeUtil.BINARY, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.BINARY));
    assertEquals(
        DatabricksTypeUtil.BINARY,
        DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.VARBINARY));
    assertEquals(
        DatabricksTypeUtil.BINARY,
        DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.LONGVARBINARY));
    assertEquals(
        DatabricksTypeUtil.DATE, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.DATE));
    assertEquals(
        DatabricksTypeUtil.DECIMAL, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.DECIMAL));
    assertEquals(
        DatabricksTypeUtil.BOOLEAN, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.BOOLEAN));
    assertEquals(
        DatabricksTypeUtil.DOUBLE, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.DOUBLE));
    assertEquals(
        DatabricksTypeUtil.FLOAT, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.FLOAT));
    assertEquals(
        DatabricksTypeUtil.TIMESTAMP_NTZ,
        DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.TIMESTAMP));
    assertEquals(
        DatabricksTypeUtil.TIMESTAMP,
        DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.TIMESTAMP_WITH_TIMEZONE));
    assertEquals(
        DatabricksTypeUtil.STRUCT, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.STRUCT));
    assertEquals(
        DatabricksTypeUtil.STRUCT, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.STRUCT));
    assertEquals(
        DatabricksTypeUtil.SMALLINT,
        DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.SMALLINT));
    assertEquals(
        DatabricksTypeUtil.TINYINT, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.TINYINT));
  }

  @Test
  void testInferDatabricksType() {
    assertEquals(DatabricksTypeUtil.BIGINT, DatabricksTypeUtil.inferDatabricksType(1L));
    assertEquals(DatabricksTypeUtil.STRING, DatabricksTypeUtil.inferDatabricksType("test"));
    assertEquals(
        DatabricksTypeUtil.TIMESTAMP,
        DatabricksTypeUtil.inferDatabricksType(new Timestamp(System.currentTimeMillis())));
    assertEquals(
        DatabricksTypeUtil.DATE,
        DatabricksTypeUtil.inferDatabricksType(new Date(System.currentTimeMillis())));
    assertEquals(DatabricksTypeUtil.VOID, DatabricksTypeUtil.inferDatabricksType(null));
    assertEquals(DatabricksTypeUtil.SMALLINT, DatabricksTypeUtil.inferDatabricksType((short) 1));
    assertEquals(DatabricksTypeUtil.TINYINT, DatabricksTypeUtil.inferDatabricksType((byte) 1));
    assertEquals(DatabricksTypeUtil.FLOAT, DatabricksTypeUtil.inferDatabricksType(1.0f));
    assertEquals(DatabricksTypeUtil.INT, DatabricksTypeUtil.inferDatabricksType(1));
    assertEquals(DatabricksTypeUtil.DOUBLE, DatabricksTypeUtil.inferDatabricksType(1.0d));
  }
}
