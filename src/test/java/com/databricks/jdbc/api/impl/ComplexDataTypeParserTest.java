package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksParsingException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ComplexDataTypeParserTest {

  private ComplexDataTypeParser parser;

  @BeforeEach
  void setUp() {
    parser = new ComplexDataTypeParser();
  }

  @Test
  void testParseJsonStringToDbArray_valid() throws DatabricksParsingException {
    String json = "[1,2,3]";

    DatabricksArray dbArray = parser.parseJsonStringToDbArray(json, "ARRAY<INT>");
    assertNotNull(dbArray);

    try {
      Object[] elements = (Object[]) dbArray.getArray();
      assertEquals(3, elements.length);
      assertEquals(1, elements[0]);
      assertEquals(2, elements[1]);
      assertEquals(3, elements[2]);
    } catch (Exception e) {
      fail("Should not throw: " + e.getMessage());
    }
  }

  @Test
  void testParseJsonStringToDbArray_invalidJson() {
    String invalidJson = "[1, 2"; // missing bracket
    Exception ex =
        assertThrows(
            DatabricksParsingException.class,
            () -> parser.parseJsonStringToDbArray(invalidJson, "ARRAY<INT>"));
    assertTrue(ex.getMessage().contains("Failed to parse JSON array from"));
  }

  @Test
  void testParseJsonStringToDbMap_valid() throws DatabricksParsingException {

    String json = "{\"k1\":100, \"k2\":200}";

    DatabricksMap<String, Object> dbMap = parser.parseJsonStringToDbMap(json, "MAP<STRING,INT>");
    assertNotNull(dbMap);
    assertEquals(2, dbMap.size());
    assertEquals(100, dbMap.get("k1"));
    assertEquals(200, dbMap.get("k2"));
  }

  @Test
  void testParseJsonStringToDbMap_invalidJson() {
    String invalidJson = "{\"k1\":100";
    Exception ex =
        assertThrows(
            DatabricksParsingException.class,
            () -> parser.parseJsonStringToDbMap(invalidJson, "MAP<STRING,INT>"));
    assertTrue(ex.getMessage().contains("Failed to parse JSON map from"));
  }

  @Test
  void testParseJsonStringToDbStruct_valid() throws DatabricksParsingException {
    String json = "{\"name\":\"Alice\", \"age\":30}";

    DatabricksStruct dbStruct =
        parser.parseJsonStringToDbStruct(json, "STRUCT<name:STRING,age:INT>");
    assertNotNull(dbStruct);

    try {
      Object[] attrs = dbStruct.getAttributes();
      // Typically the order is [name, age]
      assertEquals(2, attrs.length);
      assertEquals("Alice", attrs[0]);
      assertEquals(30, attrs[1]);
    } catch (Exception e) {
      fail("Should not throw: " + e.getMessage());
    }
  }

  @Test
  void testParseJsonStringToDbStruct_invalidJson() {
    String invalidJson = "{\"name\":\"Alice\""; // missing brace
    Exception ex =
        assertThrows(
            DatabricksParsingException.class,
            () -> parser.parseJsonStringToDbStruct(invalidJson, "STRUCT<name:STRING,age:INT>"));
    assertTrue(ex.getMessage().contains("Failed to parse JSON struct from"));
  }

  @Test
  void testComplexPrimitiveConversions() throws DatabricksParsingException {
    // We'll parse a small JSON struct that includes DECIMAL, DATE, TIME, TIMESTAMP, etc.
    String json =
        "{"
            + "\"dec\":\"123.45\","
            + "\"dt\":\"2023-10-05\","
            + "\"tm\":\"12:34:56\","
            + "\"ts\":\"2023-10-05 15:20:30\""
            + "}";

    DatabricksStruct dbStruct =
        parser.parseJsonStringToDbStruct(json, "STRUCT<dec:DECIMAL,dt:DATE,tm:TIME,ts:TIMESTAMP>");
    assertNotNull(dbStruct);

    try {
      Object[] attrs = dbStruct.getAttributes();
      assertEquals(4, attrs.length);

      // decimal => BigDecimal("123.45")
      assertEquals("123.45", attrs[0].toString());
      // date => 2023-10-05
      assertEquals(Date.valueOf("2023-10-05"), attrs[1]);
      // time => 12:34:56
      assertEquals(Time.valueOf("12:34:56"), attrs[2]);
      // timestamp => 2023-10-05 15:20:30
      assertEquals(Timestamp.valueOf("2023-10-05 15:20:30"), attrs[3]);

    } catch (Exception e) {
      fail("Should not throw: " + e.getMessage());
    }
  }

  @Test
  void testFormatComplexTypeString_withMapType() {
    String jsonString = "[{\"key\":1,\"value\":2},{\"key\":3,\"value\":4}]";
    String expected = "{1:2,3:4}";

    String result = parser.formatComplexTypeString(jsonString, "MAP", "MAP<INT,INT>");
    assertEquals(expected, result);
  }

  @Test
  void testFormatComplexTypeString_withNonMapType() {
    String jsonString = "[1,2,3]";

    String result = parser.formatComplexTypeString(jsonString, "ARRAY", "ARRAY<INT>");
    assertEquals(jsonString, result);
  }

  @Test
  void testFormatMapString_withIntKeyAndValue() throws DatabricksParsingException {
    String jsonString = "[{\"key\":1,\"value\":2},{\"key\":3,\"value\":4}]";
    String expected = "{1:2,3:4}";

    String result = parser.formatMapString(jsonString, "MAP<INT,INT>");
    assertEquals(expected, result);
  }

  @Test
  void testFormatMapString_withStringKeyAndValue() throws DatabricksParsingException {
    String jsonString = "[{\"key\":\"a\",\"value\":\"b\"},{\"key\":\"c\",\"value\":\"d\"}]";
    String expected = "{\"a\":\"b\",\"c\":\"d\"}";

    String result = parser.formatMapString(jsonString, "MAP<STRING,STRING>");
    assertEquals(expected, result);
  }

  @Test
  void testFormatMapString_withMixedTypes() throws DatabricksParsingException {
    String jsonString = "[{\"key\":\"a\",\"value\":100},{\"key\":\"b\",\"value\":200}]";
    String expected = "{\"a\":100,\"b\":200}";

    String result = parser.formatMapString(jsonString, "MAP<STRING,INT>");
    assertEquals(expected, result);
  }
}
