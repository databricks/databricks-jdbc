package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Comprehensive test cases for ComplexDataTypeParser. */
public class ComplexDataTypeParserTest {

  private static ComplexDataTypeParser parser;
  private static MockedStatic<MetadataParser> metadataParserMock;

  @BeforeAll
  public static void setUp() {
    parser = new ComplexDataTypeParser();
    metadataParserMock = Mockito.mockStatic(MetadataParser.class);
  }

  @AfterAll
  public static void tearDown() {
    metadataParserMock.close();
  }

  /**
   * Mock implementations for MetadataParser. Assuming MetadataParser has static methods
   * parseStructMetadata, parseArrayMetadata, and parseMapMetadata.
   */

  /**
   * Mock implementations for DatabricksTypeUtil. Since DatabricksTypeUtil contains only constants,
   * we can use it directly without mocking.
   */
  @Test
  public void testParseValidJson() {
    String json = "{\"key\":\"value\", \"number\":123}";
    JsonNode node = parser.parse(json);
    assertNotNull(node);
    assertTrue(node.isObject());
    assertEquals("value", node.get("key").asText());
    assertEquals(123, node.get("number").asInt());
  }

  @Test
  public void testParseInvalidJson() {
    String invalidJson = "{\"key\":\"value\", \"number\":123"; // Missing closing brace
    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> parser.parse(invalidJson));
    assertTrue(exception.getMessage().contains("Failed to parse JSON"));
  }

  @Test
  public void testParseSimpleStruct() {
    String json = "{\"name\":\"John\", \"age\":30}";
    JsonNode node = parser.parse(json);

    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("name", DatabricksTypeUtil.STRING);
    typeMap.put("age", DatabricksTypeUtil.INT);

    // Mock MetadataParser.parseStructMetadata
    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(anyString()))
        .thenReturn(typeMap);

    Map<String, Object> result = parser.parseToStruct(node, typeMap);

    assertEquals(2, result.size());
    assertEquals("John", result.get("name"));
    assertEquals(30, result.get("age"));
  }

  @Test
  public void testParseNestedStruct() {
    String json =
        "{\"name\":\"John\", \"address\":{\"street\":\"123 Main St\", \"city\":\"Anytown\"}}";
    JsonNode node = parser.parse(json);

    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("name", DatabricksTypeUtil.STRING);
    typeMap.put("address", DatabricksTypeUtil.STRUCT + "<street:string,city:string>");

    // Mock MetadataParser.parseStructMetadata for main struct
    Map<String, String> nestedTypeMap = new LinkedHashMap<>();
    nestedTypeMap.put("street", DatabricksTypeUtil.STRING);
    nestedTypeMap.put("city", DatabricksTypeUtil.STRING);

    metadataParserMock
        .when(
            () ->
                MetadataParser.parseStructMetadata(
                    DatabricksTypeUtil.STRUCT + "<street:string,city:string>"))
        .thenReturn(nestedTypeMap);

    Map<String, Object> result = parser.parseToStruct(node, typeMap);

    assertEquals(2, result.size());
    assertEquals("John", result.get("name"));
    @SuppressWarnings("unchecked")
    Map<String, Object> address = (Map<String, Object>) result.get("address");
    assertNotNull(address);
    assertEquals("123 Main St", address.get("street"));
    assertEquals("Anytown", address.get("city"));
  }

  @Test
  public void testParseToStructInvalidNode() {
    String json = "[{\"name\":\"John\"}]"; // JSON array instead of object
    JsonNode node = parser.parse(json);

    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("name", DatabricksTypeUtil.STRING);

    RuntimeException exception =
        assertThrows(IllegalArgumentException.class, () -> parser.parseToStruct(node, typeMap));
    assertTrue(exception.getMessage().contains("Expected JSON object"));
  }

  @Test
  public void testParseSimpleArray() {
    String json = "[\"apple\", \"banana\", \"cherry\"]";
    JsonNode node = parser.parse(json);

    List<Object> result = parser.parseToArray(node, DatabricksTypeUtil.STRING);

    assertEquals(3, result.size());
    assertEquals("apple", result.get(0));
    assertEquals("banana", result.get(1));
    assertEquals("cherry", result.get(2));
  }

  @Test
  public void testParseNestedArray() {
    String json = "[[1, 2], [3, 4], [5, 6]]";
    JsonNode node = parser.parse(json);

    // Mock MetadataParser.parseArrayMetadata for nested arrays
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata(DatabricksTypeUtil.ARRAY + "<int>"))
        .thenReturn(DatabricksTypeUtil.INT);

    List<Object> result = parser.parseToArray(node, DatabricksTypeUtil.ARRAY + "<int>");

    assertEquals(3, result.size());
    @SuppressWarnings("unchecked")
    List<Object> firstSubArray = (List<Object>) result.get(0);
    assertEquals(2, firstSubArray.size());
    assertEquals(1, firstSubArray.get(0));
    assertEquals(2, firstSubArray.get(1));
  }

  @Test
  public void testParseToArrayInvalidNode() {
    String json = "{\"key\":\"value\"}"; // JSON object instead of array
    JsonNode node = parser.parse(json);

    RuntimeException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> parser.parseToArray(node, DatabricksTypeUtil.STRING));
    assertTrue(exception.getMessage().contains("Expected JSON array"));
  }

  @Test
  public void testParseToMapWithObject() {
    String json = "{\"key1\":\"value1\", \"key2\":2}";
    String metadata = "map<string, string>";

    // Mock MetadataParser.parseStructMetadata for map's struct
    Map<String, String> mapTypeMap = new LinkedHashMap<>();
    mapTypeMap.put("key1", DatabricksTypeUtil.STRING);
    mapTypeMap.put("key2", DatabricksTypeUtil.STRING);

    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(anyString()))
        .thenReturn(mapTypeMap);

    Map<String, Object> result = parser.parseToMap(json, metadata);

    assertEquals(2, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("2", result.get("key2")); // Converted to string as per metadata
  }

  @Test
  public void testParseToMapWithArray() {
    String json = "[{\"key\":\"key1\", \"value\":\"value1\"}, {\"key\":\"key2\", \"value\":2}]";
    String metadata = "map<string, string>";

    // Mock MetadataParser.parseMapMetadata
    metadataParserMock
        .when(() -> MetadataParser.parseMapMetadata(anyString()))
        .thenReturn("string,string");

    // Mock MetadataParser.parseStructMetadata for map elements
    Map<String, String> mapElementTypeMap = new LinkedHashMap<>();
    mapElementTypeMap.put("key", DatabricksTypeUtil.STRING);
    mapElementTypeMap.put("value", DatabricksTypeUtil.STRING);

    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(anyString()))
        .thenReturn(mapElementTypeMap);

    Map<String, Object> result = parser.parseToMap(json, metadata);

    assertEquals(2, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("2", result.get("key2")); // Converted to string as per metadata
  }

  @Test
  public void testParseToMapInvalidJson() {
    String json = "\"just a string\""; // Not an object or array
    String metadata = "map<string, string>";

    // Adjusting the expectation to RuntimeException with IllegalArgumentException as cause
    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> parser.parseToMap(json, metadata));
    assertTrue(exception.getMessage().contains("Failed to parse JSON"));
    assertTrue(exception.getCause() instanceof IllegalArgumentException);
    assertTrue(exception.getCause().getMessage().contains("Expected JSON object or array for Map"));
  }

  /**
   * Indirectly tests type conversions through public methods. Ensures that various data types are
   * correctly parsed and converted.
   */
  @Test
  public void testTypeConversionsInStruct() {
    String json =
        "{\"intField\":123, \"longField\":1234567890123, \"floatField\":123.45, \"doubleField\":123.456, \"decimalField\":\"123.456789\", \"booleanField\":true, \"dateField\":\"2023-10-05\", \"timestampField\":\"2023-10-05 10:15:30\", \"stringField\":\"Hello\"}";
    JsonNode node = parser.parse(json);

    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("intField", DatabricksTypeUtil.INT);
    typeMap.put("longField", DatabricksTypeUtil.BIGINT);
    typeMap.put("floatField", DatabricksTypeUtil.FLOAT);
    typeMap.put("doubleField", DatabricksTypeUtil.DOUBLE);
    typeMap.put("decimalField", DatabricksTypeUtil.DECIMAL);
    typeMap.put("booleanField", DatabricksTypeUtil.BOOLEAN);
    typeMap.put("dateField", DatabricksTypeUtil.DATE);
    typeMap.put("timestampField", DatabricksTypeUtil.TIMESTAMP);
    typeMap.put("stringField", DatabricksTypeUtil.STRING);

    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(anyString()))
        .thenReturn(typeMap);

    Map<String, Object> result = parser.parseToStruct(node, typeMap);

    assertEquals(123, result.get("intField"));
    assertEquals(1234567890123L, result.get("longField"));
    assertEquals(123.45f, result.get("floatField"));
    assertEquals(123.456, result.get("doubleField"));
    assertEquals(new BigDecimal("123.456789"), result.get("decimalField"));
    assertEquals(true, result.get("booleanField"));
    assertEquals(Date.valueOf("2023-10-05"), result.get("dateField"));
    assertEquals(Timestamp.valueOf("2023-10-05 10:15:30"), result.get("timestampField"));
    assertEquals("Hello", result.get("stringField"));
  }

  /**
   * Removed tests that attempt to access private methods like parseToJavaObject. All tests now
   * interact only with public methods.
   */
  @Test
  public void testParseEmptyStruct() {
    String json = "{}";
    JsonNode node = parser.parse(json);

    Map<String, String> typeMap = new LinkedHashMap<>();
    Map<String, Object> result = parser.parseToStruct(node, typeMap);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testParseStructWithUnknownFields() {
    String json = "{\"unknownField\":\"some value\"}";
    JsonNode node = parser.parse(json);

    Map<String, String> typeMap = new LinkedHashMap<>(); // Empty type map

    // Assuming default type is STRING for unknown fields
    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(anyString()))
        .thenReturn(Collections.emptyMap());

    Map<String, Object> result = parser.parseToStruct(node, typeMap);
    assertEquals(1, result.size());
    assertEquals("some value", result.get("unknownField")); // Default to STRING
  }

  @Test
  public void testParseEmptyArray() {
    String json = "[]";
    JsonNode node = parser.parse(json);

    List<Object> result = parser.parseToArray(node, DatabricksTypeUtil.STRING);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testParseArrayWithNulls() {
    String json = "[\"value1\", null, \"value3\"]";
    JsonNode node = parser.parse(json);

    List<Object> result = parser.parseToArray(node, DatabricksTypeUtil.STRING);
    assertEquals(3, result.size());
    assertEquals("value1", result.get(0));
    assertNull(result.get(1));
    assertEquals("value3", result.get(2));
  }

  @Test
  public void testParseEmptyMap() {
    String json = "{}";
    String metadata = "map<string, string>";

    // Mock MetadataParser.parseStructMetadata
    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(anyString()))
        .thenReturn(new LinkedHashMap<>());

    Map<String, Object> result = parser.parseToMap(json, metadata);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testParseEmptyArrayMap() {
    String json = "[]";
    String metadata = "map<string, string>";

    // Mock MetadataParser.parseMapMetadata
    metadataParserMock
        .when(() -> MetadataParser.parseMapMetadata(anyString()))
        .thenReturn("string,string");

    Map<String, Object> result = parser.parseToMap(json, metadata);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testParseMapArrayMissingFields() {
    String json = "[{\"key\":\"key1\"}, {\"value\":\"value2\"}]";
    String metadata = "map<string, string>";

    // Mock MetadataParser.parseMapMetadata
    metadataParserMock
        .when(() -> MetadataParser.parseMapMetadata(anyString()))
        .thenReturn("string,string");

    // Adjusting the expectation to RuntimeException with IllegalArgumentException as cause
    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> parser.parseToMap(json, metadata));
    assertTrue(exception.getMessage().contains("Failed to parse JSON"));
    assertTrue(exception.getCause() instanceof IllegalArgumentException);
    assertTrue(
        exception
            .getCause()
            .getMessage()
            .contains("Expected array elements with 'key' and 'value' fields"));
  }
}
