package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

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
