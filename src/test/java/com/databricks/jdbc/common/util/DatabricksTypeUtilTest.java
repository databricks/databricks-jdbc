package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.model.client.thrift.generated.TTypeId;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;
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
        new Object[] {TTypeId.ARRAY_TYPE, ArrowType.Utf8.INSTANCE},
        new Object[] {TTypeId.MAP_TYPE, ArrowType.Utf8.INSTANCE},
        new Object[] {TTypeId.STRUCT_TYPE, ArrowType.Utf8.INSTANCE},
        new Object[] {TTypeId.USER_DEFINED_TYPE, ArrowType.Utf8.INSTANCE},
        new Object[] {TTypeId.DECIMAL_TYPE, ArrowType.Utf8.INSTANCE},
        new Object[] {TTypeId.DATE_TYPE, new ArrowType.Date(DateUnit.DAY)});
  }

  @ParameterizedTest
  @MethodSource("dataProvider")
  public void testMapToArrowType(TTypeId typeId, ArrowType expectedArrowType) throws SQLException {
    DatabricksTypeUtil typeUtil = new DatabricksTypeUtil(); // code coverage of constructor too
    ArrowType result = typeUtil.mapThriftToArrowType(typeId);
    assertEquals(expectedArrowType, result);
  }

  @Test
  void testGetColumnType() {
    Map<ColumnInfoTypeName, Integer> expectedMappings =
        Map.ofEntries(
            Map.entry(ColumnInfoTypeName.BYTE, Types.TINYINT),
            Map.entry(ColumnInfoTypeName.SHORT, Types.SMALLINT),
            Map.entry(ColumnInfoTypeName.SMALLINT, Types.SMALLINT),
            Map.entry(ColumnInfoTypeName.INT, Types.INTEGER),
            Map.entry(ColumnInfoTypeName.LONG, Types.BIGINT),
            Map.entry(ColumnInfoTypeName.BIGINT, Types.BIGINT),
            Map.entry(ColumnInfoTypeName.TINYINT, Types.TINYINT),
            Map.entry(ColumnInfoTypeName.VOID, Types.OTHER),
            Map.entry(ColumnInfoTypeName.FLOAT, Types.FLOAT),
            Map.entry(ColumnInfoTypeName.DOUBLE, Types.DOUBLE),
            Map.entry(ColumnInfoTypeName.DECIMAL, Types.DECIMAL),
            Map.entry(ColumnInfoTypeName.BINARY, Types.BINARY),
            Map.entry(ColumnInfoTypeName.BOOLEAN, Types.BOOLEAN),
            Map.entry(ColumnInfoTypeName.CHAR, Types.CHAR),
            Map.entry(ColumnInfoTypeName.STRING, Types.VARCHAR),
            Map.entry(ColumnInfoTypeName.MAP, Types.VARCHAR),
            Map.entry(ColumnInfoTypeName.INTERVAL, Types.VARCHAR),
            Map.entry(ColumnInfoTypeName.NULL, Types.VARCHAR),
            Map.entry(ColumnInfoTypeName.TIMESTAMP, Types.TIMESTAMP),
            Map.entry(ColumnInfoTypeName.DATE, Types.DATE),
            Map.entry(ColumnInfoTypeName.STRUCT, Types.STRUCT),
            Map.entry(ColumnInfoTypeName.ARRAY, Types.ARRAY),
            Map.entry(ColumnInfoTypeName.GEOMETRY, Types.OTHER),
            Map.entry(ColumnInfoTypeName.GEOGRAPHY, Types.OTHER),
            Map.entry(ColumnInfoTypeName.USER_DEFINED_TYPE, Types.OTHER));

    expectedMappings.forEach(
        (typeName, expectedSqlType) ->
            assertEquals(
                expectedSqlType,
                DatabricksTypeUtil.getColumnType(typeName),
                () -> "Unexpected type for " + typeName));
    assertEquals(Types.OTHER, DatabricksTypeUtil.getColumnType(null));
  }

  @Test
  void testGetColumnTypeClassName() {
    final String GEOMETRY_CLASS_NAME = "com.databricks.jdbc.api.IGeometry";
    final String GEOGRAPHY_CLASS_NAME = "com.databricks.jdbc.api.IGeography";
    Map<ColumnInfoTypeName, String> expectedMappings =
        Map.ofEntries(
            Map.entry(ColumnInfoTypeName.BYTE, "java.lang.Short"),
            Map.entry(ColumnInfoTypeName.SHORT, "java.lang.Short"),
            Map.entry(ColumnInfoTypeName.SMALLINT, "java.lang.Short"),
            Map.entry(ColumnInfoTypeName.INT, "java.lang.Integer"),
            Map.entry(ColumnInfoTypeName.TINYINT, "java.lang.Byte"),
            Map.entry(ColumnInfoTypeName.LONG, "java.lang.Long"),
            Map.entry(ColumnInfoTypeName.BIGINT, "java.lang.Long"),
            Map.entry(ColumnInfoTypeName.FLOAT, "java.lang.Float"),
            Map.entry(ColumnInfoTypeName.DOUBLE, "java.lang.Double"),
            Map.entry(ColumnInfoTypeName.DECIMAL, "java.math.BigDecimal"),
            Map.entry(ColumnInfoTypeName.BINARY, "[B"),
            Map.entry(ColumnInfoTypeName.BOOLEAN, "java.lang.Boolean"),
            Map.entry(ColumnInfoTypeName.CHAR, "java.lang.String"),
            Map.entry(ColumnInfoTypeName.STRING, "java.lang.String"),
            Map.entry(ColumnInfoTypeName.INTERVAL, "java.lang.String"),
            Map.entry(ColumnInfoTypeName.USER_DEFINED_TYPE, "java.lang.String"),
            Map.entry(ColumnInfoTypeName.TIMESTAMP, "java.sql.Timestamp"),
            Map.entry(ColumnInfoTypeName.DATE, "java.sql.Date"),
            Map.entry(ColumnInfoTypeName.STRUCT, "java.sql.Struct"),
            Map.entry(ColumnInfoTypeName.ARRAY, "java.sql.Array"),
            Map.entry(ColumnInfoTypeName.GEOMETRY, GEOMETRY_CLASS_NAME),
            Map.entry(ColumnInfoTypeName.GEOGRAPHY, GEOGRAPHY_CLASS_NAME),
            Map.entry(ColumnInfoTypeName.MAP, "java.util.Map"),
            Map.entry(ColumnInfoTypeName.NULL, "null"),
            Map.entry(ColumnInfoTypeName.VOID, "null"));

    expectedMappings.forEach(
        (columnType, expectedClassName) ->
            assertEquals(
                expectedClassName,
                DatabricksTypeUtil.getColumnTypeClassName(columnType),
                () -> "Unexpected type for " + columnType));
    assertEquals("null", DatabricksTypeUtil.getColumnTypeClassName(null));
  }

  @Test
  void testGetDisplaySize() {
    assertEquals(14, DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.FLOAT, 0, 0));
    assertEquals(24, DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.DOUBLE, 0, 0));
    assertEquals(29, DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.TIMESTAMP, 0, 0));
    assertEquals(1, DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.CHAR, 1, 0));
    assertEquals(4, DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.NULL, 1, 0));
    assertEquals(4, DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.BYTE, 1, 0));
    assertEquals(1, DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.BOOLEAN, 1, 0));
    assertEquals(10, DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.DATE, 1, 0));
    assertEquals(6, DatabricksTypeUtil.getDisplaySize(Types.SMALLINT, 5));
    assertEquals(11, DatabricksTypeUtil.getDisplaySize(Types.INTEGER, 10));
    assertEquals(5, DatabricksTypeUtil.getDisplaySize(Types.BOOLEAN, 0));
    assertEquals(1, DatabricksTypeUtil.getDisplaySize(Types.BIT, 0));
    assertEquals(128, DatabricksTypeUtil.getDisplaySize(Types.VARCHAR, 0));
    assertEquals(255, DatabricksTypeUtil.getDisplaySize(Types.OTHER, 0)); // Default case
  }

  @Test
  void testGetPrecision() {
    assertEquals(15, DatabricksTypeUtil.getPrecision(Types.DOUBLE));
    assertEquals(19, DatabricksTypeUtil.getPrecision(Types.BIGINT));
    assertEquals(3, DatabricksTypeUtil.getPrecision(Types.TINYINT));
    assertEquals(1, DatabricksTypeUtil.getPrecision(Types.BOOLEAN));
    assertEquals(7, DatabricksTypeUtil.getPrecision(Types.FLOAT));
    assertEquals(29, DatabricksTypeUtil.getPrecision(Types.TIMESTAMP));
    assertEquals(255, DatabricksTypeUtil.getPrecision(Types.STRUCT));
    assertEquals(255, DatabricksTypeUtil.getPrecision(Types.ARRAY));
    assertEquals(3, DatabricksTypeUtil.getPrecision(Types.TINYINT));
    assertEquals(5, DatabricksTypeUtil.getPrecision(Types.SMALLINT));
    assertEquals(10, DatabricksTypeUtil.getPrecision(Types.INTEGER));
  }

  @Test
  void testGetMetadataColPrecision() {
    assertEquals(5, DatabricksTypeUtil.getMetadataColPrecision(Types.SMALLINT));
    assertEquals(10, DatabricksTypeUtil.getMetadataColPrecision(Types.INTEGER));
    assertEquals(1, DatabricksTypeUtil.getMetadataColPrecision(Types.CHAR));
    assertEquals(1, DatabricksTypeUtil.getMetadataColPrecision(Types.BOOLEAN));
    assertEquals(1, DatabricksTypeUtil.getMetadataColPrecision(Types.BIT));
    assertEquals(128, DatabricksTypeUtil.getMetadataColPrecision(Types.VARCHAR));
    assertEquals(255, DatabricksTypeUtil.getMetadataColPrecision(Types.OTHER));
  }

  @Test
  void testIsSigned() {
    assertTrue(DatabricksTypeUtil.isSigned(ColumnInfoTypeName.INT));
    assertFalse(DatabricksTypeUtil.isSigned(ColumnInfoTypeName.BOOLEAN));
  }

  @Test
  void testGetDatabricksTypeFromSQLType() {
    final int UNKNOWN_TYPE = 1000;
    final String NULL = "NULL";
    Map<Integer, String> expectedMappings =
        Map.ofEntries(
            Map.entry(Types.INTEGER, DatabricksTypeUtil.INT),
            Map.entry(Types.VARCHAR, DatabricksTypeUtil.STRING),
            Map.entry(Types.CHAR, DatabricksTypeUtil.CHAR),
            Map.entry(Types.LONGVARCHAR, DatabricksTypeUtil.STRING),
            Map.entry(Types.NVARCHAR, DatabricksTypeUtil.STRING),
            Map.entry(Types.LONGNVARCHAR, DatabricksTypeUtil.STRING),
            Map.entry(Types.ARRAY, DatabricksTypeUtil.ARRAY),
            Map.entry(Types.BIGINT, DatabricksTypeUtil.LONG),
            Map.entry(Types.BINARY, DatabricksTypeUtil.BINARY),
            Map.entry(Types.VARBINARY, DatabricksTypeUtil.BINARY),
            Map.entry(Types.LONGVARBINARY, DatabricksTypeUtil.BINARY),
            Map.entry(Types.NUMERIC, DatabricksTypeUtil.DECIMAL),
            Map.entry(Types.DATE, DatabricksTypeUtil.DATE),
            Map.entry(Types.DECIMAL, DatabricksTypeUtil.DECIMAL),
            Map.entry(Types.BOOLEAN, DatabricksTypeUtil.BOOLEAN),
            Map.entry(Types.DOUBLE, DatabricksTypeUtil.DOUBLE),
            Map.entry(Types.FLOAT, DatabricksTypeUtil.FLOAT),
            Map.entry(Types.REAL, DatabricksTypeUtil.FLOAT),
            Map.entry(Types.TIMESTAMP, DatabricksTypeUtil.TIMESTAMP_NTZ),
            Map.entry(Types.TIMESTAMP_WITH_TIMEZONE, DatabricksTypeUtil.TIMESTAMP),
            Map.entry(Types.STRUCT, DatabricksTypeUtil.STRUCT),
            Map.entry(Types.SMALLINT, DatabricksTypeUtil.SMALLINT),
            Map.entry(Types.TINYINT, DatabricksTypeUtil.TINYINT),
            Map.entry(Types.BIT, DatabricksTypeUtil.BOOLEAN));

    expectedMappings.forEach(
        (sqlType, expectedType) ->
            assertEquals(
                expectedType,
                DatabricksTypeUtil.getDatabricksTypeFromSQLType(sqlType),
                () -> "Unexpected type for " + sqlType));
    assertEquals(NULL, DatabricksTypeUtil.getDatabricksTypeFromSQLType(UNKNOWN_TYPE));
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
    // Test Java 8+ temporal types
    assertEquals(
        DatabricksTypeUtil.TIMESTAMP,
        DatabricksTypeUtil.inferDatabricksType(java.time.Instant.now()));
    assertEquals(
        DatabricksTypeUtil.TIMESTAMP,
        DatabricksTypeUtil.inferDatabricksType(java.time.LocalDateTime.now()));
    assertEquals(
        DatabricksTypeUtil.DATE, DatabricksTypeUtil.inferDatabricksType(java.time.LocalDate.now()));
  }

  @ParameterizedTest
  @CsvSource({
    "STRING, STRING",
    "DATE, TIMESTAMP",
    "TIMESTAMP, TIMESTAMP",
    "TIMESTAMP_NTZ, TIMESTAMP",
    "SHORT, SHORT",
    "SMALLINT, SHORT",
    "TINYINT, TINYINT",
    "BYTE, BYTE",
    "INT, INT",
    "BIGINT, LONG",
    "LONG, LONG",
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
    "CHAR, STRING",
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

  @Test
  void testGetScale() {
    assertEquals(0, DatabricksTypeUtil.getScale(Types.DOUBLE));
    assertEquals(0, DatabricksTypeUtil.getScale(Types.FLOAT));
    assertEquals(9, DatabricksTypeUtil.getScale(Types.TIMESTAMP));
    assertEquals(0, DatabricksTypeUtil.getScale(Types.DECIMAL));
    assertEquals(0, DatabricksTypeUtil.getScale(Types.VARCHAR));
    assertEquals(0, DatabricksTypeUtil.getScale(null));
  }

  @Test
  void testGetBasePrecisionAndScale() {
    // Mock the connection context
    final int defaultStringLength = 128;
    IDatabricksConnectionContext mockContext = mock(IDatabricksConnectionContext.class);
    when(mockContext.getDefaultStringColumnLength()).thenReturn(defaultStringLength);

    // Set the mock context in thread local
    DatabricksThreadContextHolder.setConnectionContext(mockContext);

    try {
      // Test string types (should return default string length)
      int[] varcharResult = DatabricksTypeUtil.getBasePrecisionAndScale(Types.VARCHAR, mockContext);
      assertEquals(defaultStringLength, varcharResult[0]);
      assertEquals(0, varcharResult[1]);

      int[] charResult = DatabricksTypeUtil.getBasePrecisionAndScale(Types.CHAR, mockContext);
      assertEquals(defaultStringLength, charResult[0]);
      assertEquals(0, charResult[1]);

      // Test numeric types
      int[] decimalResult = DatabricksTypeUtil.getBasePrecisionAndScale(Types.DECIMAL, mockContext);
      assertEquals(10, decimalResult[0]); // Precision for DECIMAL
      assertEquals(0, decimalResult[1]); // Scale for DECIMAL

      int[] integerResult = DatabricksTypeUtil.getBasePrecisionAndScale(Types.INTEGER, mockContext);
      assertEquals(10, integerResult[0]); // Precision for INTEGER
      assertEquals(0, integerResult[1]); // Scale for INTEGER

      int[] timestampResult =
          DatabricksTypeUtil.getBasePrecisionAndScale(Types.TIMESTAMP, mockContext);
      assertEquals(29, timestampResult[0]); // Precision for TIMESTAMP
      assertEquals(9, timestampResult[1]); // Scale for TIMESTAMP
    } finally {
      // Clean up thread local
      DatabricksThreadContextHolder.clearAllContext();
    }
  }

  @Test
  void testGetDecimalTypeString() {
    // Regular case - precision > scale
    assertEquals("DECIMAL(5,2)", DatabricksTypeUtil.getDecimalTypeString(new BigDecimal("123.45")));

    // Edge case - precision = scale (all decimal digits, no integer part except 0)
    assertEquals("DECIMAL(2,2)", DatabricksTypeUtil.getDecimalTypeString(new BigDecimal("0.12")));

    // Special case - precision < scale (e.g., 0.00)
    assertEquals("DECIMAL(2,2)", DatabricksTypeUtil.getDecimalTypeString(new BigDecimal("0.00")));

    // Zero value
    assertEquals("DECIMAL(1,0)", DatabricksTypeUtil.getDecimalTypeString(BigDecimal.ZERO));

    // Large precision
    assertEquals(
        "DECIMAL(22,5)",
        DatabricksTypeUtil.getDecimalTypeString(new BigDecimal("12345678901234567.12345")));

    // Large scale
    assertEquals(
        "DECIMAL(10,10)", DatabricksTypeUtil.getDecimalTypeString(new BigDecimal("0.0123456789")));

    // Negative values
    assertEquals(
        "DECIMAL(5,2)", DatabricksTypeUtil.getDecimalTypeString(new BigDecimal("-123.45")));

    // Zero scale
    assertEquals("DECIMAL(3,0)", DatabricksTypeUtil.getDecimalTypeString(new BigDecimal("123")));

    // Scientific notation
    BigDecimal scientificNotation = new BigDecimal("1.23E-4");
    assertEquals("DECIMAL(6,6)", DatabricksTypeUtil.getDecimalTypeString(scientificNotation));

    // Very small value with trailing zeros (ensures scale is preserved)
    assertEquals(
        "DECIMAL(8,8)", DatabricksTypeUtil.getDecimalTypeString(new BigDecimal("0.00000123")));
  }
}
