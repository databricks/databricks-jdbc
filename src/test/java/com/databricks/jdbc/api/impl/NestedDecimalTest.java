package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class NestedDecimalTest {
  private final ComplexDataTypeParser parser = new ComplexDataTypeParser();

  @Test
  void defaultDecimalsUsePlainNotationWithoutChangingOtherLeaves() {
    assertEquals(
        "[0.0000000000,0.0000001200]",
        parser.formatComplexTypeString("[0E-10,1.200E-7]", "ARRAY", "ARRAY<DECIMAL(20,10)>"));
    assertEquals(
        "{\"d\":0.00000012,\"f\":1.0E+7}",
        parser.formatComplexTypeString(
            "{\"d\":1.2E-7,\"f\":1.0E7}", "STRUCT", "STRUCT<d:DECIMAL(10,8),f:DOUBLE>"));
    String json = "[1.0E+7, -0.0, 1e-7]";
    assertEquals(json, parser.formatComplexTypeString(json, "ARRAY", "ARRAY<DOUBLE>"));
    assertEquals(
        "{\"m\":[{\"key\":\"d\"}],\"d\":0.00}",
        parser.formatComplexTypeString(
            "{\"m\":[{\"key\":\"d\"}],\"d\":0E-2}",
            "STRUCT",
            "STRUCT<m:MAP<STRING,INT>,d:DECIMAL(10,2)>"));
  }

  @Test
  void decimalMapKeysUsePlainDeclaredScale() throws Exception {
    String type = "MAP<DECIMAL(38,18),INT>";
    for (String json : new String[] {"{\"0E-18\":1}", "[{\"key\":0E-18,\"value\":1}]"}) {
      Map<String, Object> map = parser.parseJsonStringToDbMap(json, type);
      assertEquals(1, map.get("0.000000000000000000"));
      assertEquals("{\"0.000000000000000000\":1}", map.toString());
    }
  }

  @Test
  void decimalRoundingMatchesTopLevel() throws Exception {
    assertEquals(
        new BigDecimal("1.24"),
        ((Object[]) parser.parseJsonStringToDbArray("[1.235]", "ARRAY<DECIMAL(10,2)>").getArray())
            [0]);
    assertEquals(
        "{\"d\":1.24}",
        parser.formatMapString("[{\"key\":\"d\",\"value\":1.235}]", "MAP<STRING,DECIMAL(10,2)>"));
  }

  @Test
  void malformedDecimalsHaveDriverParsingErrors() {
    for (String json : new String[] {"[\"abc\"]", "", "null", "[1]", "[\"1E+2147483647\"]"}) {
      String type = json.equals("[1]") ? "ARRAY<DECIMAL(10,2147483648)>" : "ARRAY<DECIMAL(10,2)>";
      DatabricksParsingException error =
          assertThrows(
              DatabricksParsingException.class, () -> parser.parseJsonStringToDbArray(json, type));
      assertEquals(DatabricksDriverErrorCode.JSON_PARSING_ERROR.getCode(), error.getErrorCode());
      assertEquals(DatabricksDriverErrorCode.JSON_PARSING_ERROR.name(), error.getSQLState());
    }
  }

  @Test
  void cachedPlansIgnoreFieldNames() {
    String type = "STRUCT<decision:BIGINT,description:STRING,`DECIMAL`:DOUBLE>";
    ComplexTypePlan plan = ComplexTypePlan.of(type);
    assertFalse(plan.containsDecimal);
    assertSame(plan, ComplexTypePlan.of(type));
    assertTrue(ComplexTypePlan.of("ARRAY<STRUCT<d:decimal(20,10)>>").containsDecimal);
    String json = "{\"decision\":9007199254740993,\"description\":\"x\",\"DECIMAL\":1.0E7}";
    assertEquals(
        "{\"decision\":9007199254740993,\"description\":\"x\",\"DECIMAL\":1.0E+7}",
        parser.formatComplexTypeString(json, "STRUCT", type));
  }

  @Test
  void parsesEachTypeOnceAcrossRows() throws Exception {
    String type = "ARRAY<STRUCT<cached_decimal:DECIMAL(25,10)>>";
    ComplexTypePlan.CACHE.invalidateAll();
    try (org.mockito.MockedStatic<MetadataParser> metadata =
        org.mockito.Mockito.mockStatic(
            MetadataParser.class, org.mockito.Mockito.CALLS_REAL_METHODS)) {
      for (int row = 0; row < 3; row++) {
        parser.parseJsonStringToDbArray(
            "[{\"cached_decimal\":1.00},{\"cached_decimal\":2.00}]", type);
      }
      metadata.verify(() -> MetadataParser.parseArrayMetadata(type), org.mockito.Mockito.times(1));
      metadata.verify(
          () -> MetadataParser.parseStructMetadata("STRUCT<cached_decimal:DECIMAL(25,10)>"),
          org.mockito.Mockito.times(1));
    } finally {
      ComplexTypePlan.CACHE.invalidateAll();
    }
  }

  @ParameterizedTest
  @CsvSource({
    "1.10000,15,5",
    "1234567890.12345,15,5",
    "100.00,10,2",
    "12345678901234567890.0123456789,38,10",
    "-12345678901234567890.0123456789,38,10",
    "99999999999999999999999999999999999999,38,0",
    "0.00000000000000000000000000000000000001,38,38",
    "0.0000000000,38,10"
  })
  void preservesDecimalLeaves(String text, int precision, int scale) throws Exception {
    String type = "DECIMAL(" + precision + "," + scale + ")";
    BigDecimal expected = new BigDecimal(text);
    DatabricksArray array =
        parser.parseJsonStringToDbArray("[" + text + ",null]", "ARRAY<" + type + ">");
    assertArrayEquals(new Object[] {expected, null}, (Object[]) array.getArray());
    assertEquals("[" + text + ",null]", array.toString());
    DatabricksStruct struct =
        parser.parseJsonStringToDbStruct(
            "{\"d\":" + text + ",\"n\":null}", "STRUCT<d:" + type + ",n:" + type + ">");
    assertArrayEquals(new Object[] {expected, null}, struct.getAttributes());
    assertEquals("{\"d\":" + text + ",\"n\":null}", struct.toString());
    DatabricksMap<String, Object> map =
        parser.parseJsonStringToDbMap(
            "[{\"key\":\"d\",\"value\":" + text + "},{\"key\":\"n\",\"value\":null}]",
            "MAP<STRING," + type + ">");
    assertEquals(expected, map.get("d"));
    assertNull(map.get("n"));
    assertEquals("{\"d\":" + text + ",\"n\":null}", map.toString());
  }

  @ParameterizedTest
  @ValueSource(strings = {"DECIMAL(10,2)", "decimal(10, 2)", "DEC(10,2)", "NUMERIC(10,2)"})
  void appliesDeclaredScaleInAllContainers(String type) throws Exception {
    BigDecimal expected = new BigDecimal("100.00");
    assertEquals(
        expected,
        ((Object[]) parser.parseJsonStringToDbArray("[100]", "ARRAY<" + type + ">").getArray())[0]);
    assertEquals(
        expected,
        new DatabricksStruct(Collections.singletonMap("d", "100"), "STRUCT<d:" + type + ">")
            .getAttributes()[0]);
    assertEquals(
        expected,
        new DatabricksMap<>(Collections.singletonMap("d", "100"), "MAP<STRING," + type + ">")
            .get("d"));
    assertEquals(
        expected,
        ((Object[])
                new DatabricksArray(Collections.singletonList("100"), "ARRAY<" + type + ">")
                    .getArray())
            [0]);
  }

  @Test
  void preservesNestedInNestedDecimals() throws Exception {
    String text = "12345678901234567890.0123456789";
    BigDecimal expected = new BigDecimal(text);
    Array array =
        parser.parseJsonStringToDbArray(
            "[{\"d\":" + text + "},null]", "ARRAY<STRUCT<d:DECIMAL(38,10)>>");
    Object[] elements = (Object[]) array.getArray();
    assertEquals(expected, ((Struct) elements[0]).getAttributes()[0]);
    assertNull(elements[1]);
    assertEquals("[{\"d\":" + text + "},null]", array.toString());
    Map<String, Object> map =
        parser.parseJsonStringToDbMap(
            "{\"d\":[" + text + ",null]}", "MAP<STRING,ARRAY<DECIMAL(38,10)>>");
    assertArrayEquals(new Object[] {expected, null}, (Object[]) ((Array) map.get("d")).getArray());
    assertEquals("{\"d\":[" + text + ",null]}", map.toString());
  }

  @Test
  void preservesDefaultMapDecimalsAndEscaping() {
    assertEquals(
        "{\"d\":100.00}",
        parser.formatMapString("[{\"key\":\"d\",\"value\":\"100\"}]", "MAP<STRING,DECIMAL(10,2)>"));
    String json = "[{\"key\":\"a\\\"\\n\",\"value\":100.00}]";
    assertEquals(
        "{\"a\\\"\\n\":100.00}", parser.formatMapString(json, "MAP<STRING,DECIMAL(10,2)>"));
    assertEquals(
        "{\"d\":[12345678901234567890.0123456789,null]}",
        parser.formatComplexTypeString(
            "[{\"key\":\"d\",\"value\":[12345678901234567890.0123456789,null]}]",
            "MAP",
            "MAP<STRING,ARRAY<DECIMAL(38,10)>>"));
  }

  @Test
  void keepsFloatingPointFormattingBesideDecimalLeaves() {
    String input =
        "[{\"key\":\"m\",\"value\":{\"d\":100.00,\"f\":1.0E7,\"a\":[1.0E-7],\"s\":\"a\\\"\\n\"}}]";
    assertEquals(
        "{\"m\":{\"d\":100.00,\"f\":1.0E7,\"a\":[1.0E-7],\"s\":\"a\\\"\\n\"}}",
        parser.formatMapString(
            input, "MAP<STRING,STRUCT<d:DECIMAL(10,2),f:DOUBLE,a:ARRAY<FLOAT>,s:STRING>>"));
    assertEquals(
        "{\"m\":{\"d\":0.00000001,\"f\":1.0E7}}",
        parser.formatMapString(
            "[{\"key\":\"m\",\"value\":{\"d\":1E-8,\"f\":1.0E7}}]",
            "MAP<STRING,STRUCT<d:DECIMAL(10,8),f:DOUBLE>>"));
  }

  @Test
  void leavesFloatingPointTypesUnchanged() throws Exception {
    assertEquals("[-0.0]", parser.parseJsonStringToDbArray("[-0.0]", "ARRAY<DOUBLE>").toString());
    assertEquals(
        "{\"d\":1.00,\"f\":-0.0}",
        parser
            .parseJsonStringToDbStruct(
                "{\"d\":1.00,\"f\":-0.0}", "STRUCT<d:DECIMAL(10,2),f:DOUBLE>")
            .toString());
    assertEquals(
        "{\"m\":{\"d\":1.00,\"f\":-0.0}}",
        parser.formatMapString(
            "[{\"key\":\"m\",\"value\":{\"d\":1.00,\"f\":-0.0}}]",
            "MAP<STRING,STRUCT<d:DECIMAL(10,2),f:DOUBLE>>"));
    assertArrayEquals(
        new Object[] {1.1d, 1.23456789012345E9, null},
        (Object[])
            parser
                .parseJsonStringToDbArray("[1.1,1234567890.12345,null]", "ARRAY<DOUBLE>")
                .getArray());
    assertArrayEquals(
        new Object[] {1.1f, null},
        (Object[]) parser.parseJsonStringToDbArray("[1.1,null]", "ARRAY<FLOAT>").getArray());
    assertEquals(
        "[1.1,1.23456789012345E9,null]",
        parser.parseJsonStringToDbArray("[1.1,1234567890.12345,null]", "ARRAY<DOUBLE>").toString());
    assertEquals(
        "{\"d\":1.0E7}",
        parser.formatMapString("[{\"key\":\"d\",\"value\":1.0E7}]", "MAP<STRING,DOUBLE>"));
    assertEquals(
        "{\"d\":[1.0E-7]}",
        parser.formatMapString(
            "[{\"key\":\"d\",\"value\":[1.0E-7]}]", "MAP<STRING,ARRAY<DOUBLE>>"));
    assertEquals(
        "{\"d\":1.1,\"f\":1.1}",
        parser
            .parseJsonStringToDbStruct("{\"d\":1.1,\"f\":1.1}", "STRUCT<d:DOUBLE,f:FLOAT>")
            .toString());
  }
}
