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
  void testComplexPrimitiveTimestampWithOffset() throws DatabricksParsingException {
    String json = "{\"ts\":\"2025-03-18T12:08:31.552223-07:00\"}";

    DatabricksStruct dbStruct = parser.parseJsonStringToDbStruct(json, "STRUCT<ts:TIMESTAMP>");
    assertNotNull(dbStruct);

    try {
      Object[] attrs = dbStruct.getAttributes();
      assertEquals(1, attrs.length);
      Timestamp ts = (Timestamp) attrs[0];
      assertNotNull(ts);
      // TimestampConverter converts offset timestamps to local time, ignoring the offset
      // Verify the local datetime components instead of epoch millis to avoid timezone issues
      assertEquals("2025-03-18 12:08:31.552223", ts.toString());
    } catch (Exception e) {
      fail("Should not throw: " + e.getMessage());
    }
  }

  @Test
  void testDateAsEpochDayInStruct() throws DatabricksParsingException {
    // Reproduces GitHub issue #1247: Date fields within ARRAY<STRUCT> are serialized
    // as epoch day integers instead of ISO-8601 strings when coming from Arrow.
    // Arrow's getObject() on nested types returns epoch day integers for DATE fields.
    // 20487 = epoch day for 2026-02-03
    String json = "[{\"event_date\":20487}]";

    DatabricksArray dbArray =
        parser.parseJsonStringToDbArray(json, "ARRAY<STRUCT<event_date:DATE>>");
    assertNotNull(dbArray);

    try {
      Object[] elements = (Object[]) dbArray.getArray();
      assertEquals(1, elements.length);
      DatabricksStruct struct = (DatabricksStruct) elements[0];
      Object[] attrs = struct.getAttributes();
      assertEquals(1, attrs.length);
      assertEquals(Date.valueOf("2026-02-03"), attrs[0]);
    } catch (Exception e) {
      fail("Should not throw: " + e.getMessage());
    }
  }

  @Test
  void testDateAsEpochDayInArray() throws DatabricksParsingException {
    // DATE inside a plain ARRAY — Arrow returns epoch day integers
    String json = "[20487, 20488]";

    DatabricksArray dbArray = parser.parseJsonStringToDbArray(json, "ARRAY<DATE>");
    assertNotNull(dbArray);

    try {
      Object[] elements = (Object[]) dbArray.getArray();
      assertEquals(2, elements.length);
      assertEquals(Date.valueOf("2026-02-03"), elements[0]);
      assertEquals(Date.valueOf("2026-02-04"), elements[1]);
    } catch (Exception e) {
      fail("Should not throw: " + e.getMessage());
    }
  }

  @Test
  void testDateAsEpochDayInMap() throws DatabricksParsingException {
    // DATE as value in a MAP — Arrow returns epoch day integers
    String json = "{\"key1\":20487, \"key2\":20488}";

    DatabricksMap<String, Object> dbMap = parser.parseJsonStringToDbMap(json, "MAP<STRING,DATE>");
    assertNotNull(dbMap);

    assertEquals(Date.valueOf("2026-02-03"), dbMap.get("key1"));
    assertEquals(Date.valueOf("2026-02-04"), dbMap.get("key2"));
  }

  @Test
  void testInvalidDateStringInStructThrowsOriginalException() {
    // Non-numeric invalid date string should throw IllegalArgumentException, not
    // NumberFormatException
    String json = "[{\"event_date\":\"2026/02/03\"}]";

    assertThrows(
        IllegalArgumentException.class,
        () -> parser.parseJsonStringToDbArray(json, "ARRAY<STRUCT<event_date:DATE>>"));
  }

  @Test
  void testDateAsStringInStruct() throws DatabricksParsingException {
    // Ensure ISO-8601 date strings still work in nested structs
    String json = "[{\"event_date\":\"2026-02-03\"}]";

    DatabricksArray dbArray =
        parser.parseJsonStringToDbArray(json, "ARRAY<STRUCT<event_date:DATE>>");
    assertNotNull(dbArray);

    try {
      Object[] elements = (Object[]) dbArray.getArray();
      assertEquals(1, elements.length);
      DatabricksStruct struct = (DatabricksStruct) elements[0];
      Object[] attrs = struct.getAttributes();
      assertEquals(1, attrs.length);
      assertEquals(Date.valueOf("2026-02-03"), attrs[0]);
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
