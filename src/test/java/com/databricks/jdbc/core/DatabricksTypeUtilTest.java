package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.client.impl.thrift.generated.TTypeId;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class DatabricksTypeUtilTest {
  static Stream<Object[]> dataProvider() {
    return Stream.of(
        new Object[] {TTypeId.BOOLEAN_TYPE, ArrowType.Bool.INSTANCE},
        new Object[] {TTypeId.TINYINT_TYPE, new ArrowType.Int(8, true)},
        new Object[] {TTypeId.SMALLINT_TYPE, new ArrowType.Int(16, true)},
        new Object[] {TTypeId.INT_TYPE, new ArrowType.Int(32, true)},
        new Object[] {TTypeId.BIGINT_TYPE, new ArrowType.Int(64, true)},
        new Object[] {
          TTypeId.FLOAT_TYPE, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
        },
        new Object[] {
          TTypeId.DOUBLE_TYPE, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
        },
        new Object[] {TTypeId.STRING_TYPE, ArrowType.Utf8.INSTANCE},
        new Object[] {TTypeId.INTERVAL_DAY_TIME_TYPE, ArrowType.Utf8.INSTANCE},
        new Object[] {TTypeId.INTERVAL_YEAR_MONTH_TYPE, ArrowType.Utf8.INSTANCE},
        new Object[] {TTypeId.UNION_TYPE, ArrowType.Utf8.INSTANCE},
        new Object[] {TTypeId.STRING_TYPE, ArrowType.Utf8.INSTANCE},
        new Object[] {TTypeId.VARCHAR_TYPE, ArrowType.Utf8.INSTANCE},
        new Object[] {TTypeId.CHAR_TYPE, ArrowType.Utf8.INSTANCE},
        new Object[] {TTypeId.TIMESTAMP_TYPE, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)},
        new Object[] {TTypeId.BINARY_TYPE, ArrowType.Binary.INSTANCE},
        new Object[] {TTypeId.NULL_TYPE, ArrowType.Null.INSTANCE},
        new Object[] {TTypeId.DATE_TYPE, new ArrowType.Date(DateUnit.DAY)});
  }

  @ParameterizedTest
  @MethodSource("dataProvider")
  public void testMapToArrowType(TTypeId typeId, ArrowType expectedArrowType)
      throws DatabricksSQLException {
    DatabricksTypeUtil typeUtil = new DatabricksTypeUtil(); // code coverage of constructor too
    ArrowType result = typeUtil.mapThriftToArrowType(typeId);
    assertEquals(expectedArrowType, result);
  }

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
    assertEquals(5, DatabricksTypeUtil.getPrecision(ColumnInfoTypeName.BYTE));
    assertEquals(5, DatabricksTypeUtil.getPrecision(ColumnInfoTypeName.SHORT));
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

  @ParameterizedTest
  @CsvSource({
    "STRING, STRING",
    "DATE, TIMESTAMP",
    "TIMESTAMP, TIMESTAMP",
    "TIMESTAMP_NTZ, TIMESTAMP",
    "SHORT, SHORT",
    "TINYINT, SHORT",
    "BYTE, BYTE",
    "INT, INT",
    "BIGINT, LONG",
    "FLOAT, FLOAT",
    "DOUBLE, DOUBLE",
    "BINARY, BINARY",
    "BOOLEAN, BOOLEAN",
    "DECIMAL, DECIMAL",
    "STRUCT, STRUCT",
    "ARRAY, ARRAY",
    "VOID, NULL",
    "NULL, NULL",
    "MAP, MAP",
    "UNKNOWN, USER_DEFINED_TYPE"
  })
  public void testGetColumnInfoType(String inputTypeName, String expectedTypeName) {
    assertEquals(
        ColumnInfoTypeName.valueOf(expectedTypeName),
        DatabricksTypeUtil.getColumnInfoType(inputTypeName),
        String.format(
            "inputType : %s, output should have been %s.  But was %s",
            inputTypeName, expectedTypeName, DatabricksTypeUtil.getColumnInfoType(inputTypeName)));
  }

  @ParameterizedTest
  @CsvSource({"FLOAT, 0", "DOUBLE, 0", "DECIMAL, 0", "TIMESTAMP, 9", "STRING, 0", "NULL, 0"})
  void testGetScale(ColumnInfoTypeName typeName, int expectedScale) {
    assertEquals(
        expectedScale,
        DatabricksTypeUtil.getScale(typeName),
        "Scale did not match for type: " + typeName);
  }
}
