package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.exception.DatabricksDriverException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Comprehensive and non-redundant test cases for DatabricksStruct. */
public class DatabricksStructTest {

  private MockedStatic<MetadataParser> metadataParserMock;

  @BeforeEach
  public void setUp() {
    // Initialize Mockito's MockedStatic for MetadataParser before each test
    metadataParserMock = Mockito.mockStatic(MetadataParser.class);
  }

  @AfterEach
  public void tearDown() {
    // Close the MockedStatic context after each test
    metadataParserMock.close();
  }

  /**
   * Helper method to mock MetadataParser.parseStructMetadata based on the struct metadata string.
   *
   * @param structMetadata the metadata string describing the struct
   * @param returnTypeMap the map representing parsed struct metadata
   */
  private void mockParseStructMetadata(String structMetadata, Map<String, String> returnTypeMap) {
    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(structMetadata))
        .thenReturn(returnTypeMap);
  }

  /** Test the constructor with valid simple types. */
  @Test
  public void constructor_ShouldConvertSimpleTypesCorrectly() throws SQLException {
    String metadata = "STRUCT<id:INT,name:STRING,active:BOOLEAN>";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "1");
    attributes.put("name", "Alice");
    attributes.put("active", "true");

    // Mock MetadataParser.parseStructMetadata to return a map of field types
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    typeMap.put("name", "STRING");
    typeMap.put("active", "BOOLEAN");
    mockParseStructMetadata(metadata, typeMap);

    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);

    // Retrieve attributes
    Object[] convertedAttributes = databricksStruct.getAttributes();

    // Assertions
    assertNotNull(convertedAttributes, "Converted attributes should not be null");
    assertEquals(3, convertedAttributes.length, "Struct should have three attributes");
    assertEquals(
        1, convertedAttributes[0], "First attribute 'id' should be converted to Integer 1");
    assertEquals("Alice", convertedAttributes[1], "Second attribute 'name' should be 'Alice'");
    assertEquals(true, convertedAttributes[2], "Third attribute 'active' should be true");

    // Verify that parseStructMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /** Test the constructor with nested Struct elements. */
  @Test
  public void constructor_ShouldHandleNestedStructsProperly() throws SQLException {
    String metadata = "STRUCT<id:INT,address:STRUCT<street:STRING,city:STRING>>";
    Map<String, Object> address = new HashMap<>();
    address.put("street", "123 Main St");
    address.put("city", "Springfield");

    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "100");
    attributes.put("address", address);

    // Mock MetadataParser.parseStructMetadata for the outer struct
    Map<String, String> outerTypeMap = new LinkedHashMap<>();
    outerTypeMap.put("id", "INT");
    outerTypeMap.put("address", "STRUCT<street:STRING,city:STRING>");
    mockParseStructMetadata(metadata, outerTypeMap);

    // Mock MetadataParser.parseStructMetadata for the nested struct
    Map<String, String> innerTypeMap = new LinkedHashMap<>();
    innerTypeMap.put("street", "STRING");
    innerTypeMap.put("city", "STRING");
    mockParseStructMetadata("STRUCT<street:STRING,city:STRING>", innerTypeMap);

    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);

    // Retrieve attributes
    Object[] convertedAttributes = databricksStruct.getAttributes();

    // Assertions
    assertNotNull(convertedAttributes, "Converted attributes should not be null");
    assertEquals(2, convertedAttributes.length, "Struct should have two attributes");

    // Verify first attribute
    assertEquals(
        100, convertedAttributes[0], "First attribute 'id' should be converted to Integer 100");

    // Verify nested struct
    assertTrue(
        convertedAttributes[1] instanceof DatabricksStruct,
        "Second attribute 'address' should be a DatabricksStruct");
    DatabricksStruct addressStruct = (DatabricksStruct) convertedAttributes[1];
    Object[] addressAttributes = addressStruct.getAttributes();
    assertEquals(2, addressAttributes.length, "Nested struct should have two attributes");
    assertEquals(
        "123 Main St", addressAttributes[0], "Nested attribute 'street' should be '123 Main St'");
    assertEquals(
        "Springfield", addressAttributes[1], "Nested attribute 'city' should be 'Springfield'");

    // Verify that parseStructMetadata was called twice
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
    metadataParserMock.verify(
        () -> MetadataParser.parseStructMetadata("STRUCT<street:STRING,city:STRING>"), times(1));
  }

  //    /**
  //     * Test the constructor with nested Array elements.
  //     */
  //    @Test
  //    public void constructor_ShouldHandleNestedArraysProperly() throws SQLException {
  //        String metadata = "STRUCT<id:INT, tags:ARRAY<STRING>>";
  //        List<Object> tags = Arrays.asList("urgent", "finance");
  //
  //        Map<String, Object> attributes = new HashMap<>();
  //        attributes.put("id", "200");
  //        attributes.put("tags", tags);
  //
  //        // Mock MetadataParser.parseStructMetadata for the outer struct
  //        Map<String, String> outerTypeMap = new LinkedHashMap<>();
  //        outerTypeMap.put("id", "INT");
  //        outerTypeMap.put("tags", "ARRAY<STRING>");
  //        mockParseStructMetadata(metadata, outerTypeMap);
  //
  //        // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
  //        metadataParserMock.when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
  //                .thenReturn("STRING");
  //
  //        DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);
  //
  //        // Retrieve attributes
  //        Object[] convertedAttributes = databricksStruct.getAttributes();
  //
  //        // Assertions
  //        assertNotNull(convertedAttributes, "Converted attributes should not be null");
  //        assertEquals(2, convertedAttributes.length, "Struct should have two attributes");
  //        assertEquals(200, convertedAttributes[0], "Attribute 'id' should be converted to Integer
  // 200");
  //
  //        // Verify nested array
  //        assertTrue(convertedAttributes[1] instanceof DatabricksArray, "Attribute 'tags' should
  // be a DatabricksArray");
  //        DatabricksArray tagsArray = (DatabricksArray) convertedAttributes[1];
  //        Object[] tagsElements = tagsArray.getArray();
  //        assertEquals(2, tagsElements.length, "Nested array 'tags' should have two elements");
  //        assertEquals("urgent", tagsElements[0], "First tag should be 'urgent'");
  //        assertEquals("finance", tagsElements[1], "Second tag should be 'finance'");
  //
  //        // Verify that parseStructMetadata was called once and parseArrayMetadata was called
  // once
  //        metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  //        metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"),
  // times(1));
  //    }

  /** Test the constructor with nested Map elements. */
  @Test
  public void constructor_ShouldHandleNestedMapsProperly() throws SQLException {
    String metadata = "STRUCT<id:INT, preferences:MAP<STRING, STRING>>";
    Map<String, Object> preferences = new HashMap<>();
    preferences.put("theme", "dark");
    preferences.put("notifications", "enabled");

    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "300");
    attributes.put("preferences", preferences);

    // Mock MetadataParser.parseStructMetadata for the outer struct
    Map<String, String> outerTypeMap = new LinkedHashMap<>();
    outerTypeMap.put("id", "INT");
    outerTypeMap.put("preferences", "MAP<STRING, STRING>");
    mockParseStructMetadata(metadata, outerTypeMap);

    // Mock MetadataParser.parseMapMetadata for "MAP<STRING, STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseMapMetadata("MAP<STRING, STRING>"))
        .thenReturn("STRING,STRING");

    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);

    // Retrieve attributes
    Object[] convertedAttributes = databricksStruct.getAttributes();

    // Assertions
    assertNotNull(convertedAttributes, "Converted attributes should not be null");
    assertEquals(2, convertedAttributes.length, "Struct should have two attributes");
    assertEquals(300, convertedAttributes[0], "Attribute 'id' should be converted to Integer 300");

    // Verify nested map
    assertTrue(
        convertedAttributes[1] instanceof DatabricksMap,
        "Attribute 'preferences' should be a DatabricksMap");
    DatabricksMap<String, String> preferencesMap =
        (DatabricksMap<String, String>) convertedAttributes[1];
    // Assuming DatabricksMap implements Map, we can cast it to Map directly
    Map<String, String> mapEntries = (Map<String, String>) preferencesMap;
    assertEquals(2, mapEntries.size(), "Nested map 'preferences' should have two entries");
    assertEquals("dark", mapEntries.get("theme"), "Preference 'theme' should be 'dark'");
    assertEquals(
        "enabled",
        mapEntries.get("notifications"),
        "Preference 'notifications' should be 'enabled'");

    // Verify that parseStructMetadata was called once and parseMapMetadata was called once
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
    metadataParserMock.verify(
        () -> MetadataParser.parseMapMetadata("MAP<STRING, STRING>"), times(1));
  }

  /**
   * Test the constructor with invalid Struct element (providing a non-Map). Expects
   * DatabricksDriverException.
   */
  @Test
  public void constructor_ShouldThrowException_WhenStructElementIsInvalid() throws SQLException {
    String metadata = "STRUCT<id:INT, address:STRUCT<street:STRING, city:STRING>>";
    // Providing a non-Map element for 'address'
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "400");
    attributes.put("address", "InvalidAddress"); // Should be a Map

    // Mock MetadataParser.parseStructMetadata for the outer struct
    Map<String, String> outerTypeMap = new LinkedHashMap<>();
    outerTypeMap.put("id", "INT");
    outerTypeMap.put("address", "STRUCT<street:STRING,city:STRING>");
    mockParseStructMetadata(metadata, outerTypeMap);

    // Mock MetadataParser.parseStructMetadata for the nested struct
    Map<String, String> innerTypeMap = new LinkedHashMap<>();
    innerTypeMap.put("street", "STRING");
    innerTypeMap.put("city", "STRING");
    mockParseStructMetadata("STRUCT<street:STRING,city:STRING>", innerTypeMap);

    // Expecting DatabricksDriverException due to 'address' being a String instead of a Map
    DatabricksDriverException exception =
        assertThrows(
            DatabricksDriverException.class,
            () -> {
              new DatabricksStruct(attributes, metadata);
            },
            "Constructor should throw DatabricksDriverException when struct element is invalid");

    assertTrue(
        exception.getMessage().contains("Expected a Map for STRUCT but found: String"),
        "Exception message should indicate expected STRUCT but found String");

    // Verify that parseStructMetadata was called once for the outer struct and once for the nested
    // struct
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /**
   * Test the constructor with invalid Array element (providing a non-List). Expects
   * DatabricksDriverException.
   */
  @Test
  public void constructor_ShouldThrowException_WhenArrayElementIsInvalid() throws SQLException {
    String metadata = "STRUCT<id:INT, tags:ARRAY<STRING>>";
    // Providing a non-List element for 'tags'
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "500");
    attributes.put("tags", "InvalidTags"); // Should be a List

    // Mock MetadataParser.parseStructMetadata for the struct
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    typeMap.put("tags", "ARRAY<STRING>");
    mockParseStructMetadata(metadata, typeMap);

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    // Expecting DatabricksDriverException due to 'tags' being a String instead of a List
    DatabricksDriverException exception =
        assertThrows(
            DatabricksDriverException.class,
            () -> {
              new DatabricksStruct(attributes, metadata);
            },
            "Constructor should throw DatabricksDriverException when array element is invalid");

    assertTrue(
        exception.getMessage().contains("Expected a List for ARRAY but found: String"),
        "Exception message should indicate expected ARRAY but found String");

    // Verify that parseStructMetadata was called once and parseArrayMetadata was called once
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /** Test the constructor with null metadata. Expects NullPointerException. */
  @Test
  public void constructor_ShouldThrowNullPointerException_WhenMetadataIsNull() throws SQLException {
    String metadata = null;
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "600");
    attributes.put("name", "Charlie");

    // Mock MetadataParser.parseStructMetadata to throw exception when metadata is null
    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(null))
        .thenThrow(new NullPointerException("Metadata cannot be null"));

    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> {
              new DatabricksStruct(attributes, metadata);
            },
            "Constructor should throw NullPointerException when metadata is null");

    assertEquals(
        "Metadata cannot be null",
        exception.getMessage(),
        "Exception message should indicate that metadata cannot be null");

    // Verify that parseStructMetadata was called once with null metadata
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /** Test the constructor with empty attributes map. */
  @Test
  public void constructor_ShouldHandleEmptyAttributesMapProperly() throws SQLException {
    String metadata = "STRUCT<id:INT,name:STRING>";
    Map<String, Object> attributes = new HashMap<>();

    // Mock MetadataParser.parseStructMetadata to return a map of field types
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    typeMap.put("name", "STRING");
    mockParseStructMetadata(metadata, typeMap);

    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);

    // Retrieve attributes
    Object[] convertedAttributes = databricksStruct.getAttributes();

    // Assertions
    assertNotNull(convertedAttributes, "Converted attributes should not be null");
    assertEquals(2, convertedAttributes.length, "Struct should have two attributes");
    assertNull(convertedAttributes[0], "Attribute 'id' should be null");
    assertNull(convertedAttributes[1], "Attribute 'name' should be null");

    // Verify that parseStructMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /** Test the constructor with elements requiring type conversion from String to Integer. */
  @Test
  public void constructor_ShouldConvertStringToIntegerSuccessfully() throws SQLException {
    String metadata = "STRUCT<id:INT>";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "800"); // String that needs to be converted to Integer

    // Mock MetadataParser.parseStructMetadata to return a map of field types
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    mockParseStructMetadata(metadata, typeMap);

    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);

    // Retrieve attributes
    Object[] convertedAttributes = databricksStruct.getAttributes();

    // Assertions
    assertNotNull(convertedAttributes, "Converted attributes should not be null");
    assertEquals(1, convertedAttributes.length, "Struct should have one attribute");
    assertEquals(800, convertedAttributes[0], "Attribute 'id' should be converted to Integer 800");

    // Verify that parseStructMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /** Test the constructor with binary type elements. */
  @Test
  public void constructor_ShouldHandleBinaryElementsCorrectly() throws SQLException {
    String metadata = "STRUCT<id:INT,data:BINARY>";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "1300");
    attributes.put("data", "binaryData".getBytes());

    // Mock MetadataParser.parseStructMetadata to return a map of field types
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    typeMap.put("data", "BINARY");
    mockParseStructMetadata(metadata, typeMap);

    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);

    // Retrieve attributes
    Object[] convertedAttributes = databricksStruct.getAttributes();

    // Assertions
    assertNotNull(convertedAttributes, "Converted attributes should not be null");
    assertEquals(2, convertedAttributes.length, "Struct should have two attributes");
    assertEquals(
        1300, convertedAttributes[0], "Attribute 'id' should be converted to Integer 1300");
    assertArrayEquals(
        "binaryData".getBytes(),
        (byte[]) convertedAttributes[1],
        "Attribute 'data' should match the binary data");

    // Verify that parseStructMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /** Test the constructor with DATE type elements. */
  @Test
  public void constructor_ShouldConvertStringToDateSuccessfully() throws SQLException {
    String metadata = "STRUCT<id:INT,birthdate:DATE>";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "1400");
    attributes.put("birthdate", "1990-05-15");

    // Mock MetadataParser.parseStructMetadata to return a map of field types
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    typeMap.put("birthdate", "DATE");
    mockParseStructMetadata(metadata, typeMap);

    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);

    // Retrieve attributes
    Object[] convertedAttributes = databricksStruct.getAttributes();

    // Assertions
    assertNotNull(convertedAttributes, "Converted attributes should not be null");
    assertEquals(2, convertedAttributes.length, "Struct should have two attributes");
    assertEquals(
        1400, convertedAttributes[0], "Attribute 'id' should be converted to Integer 1400");
    assertEquals(
        Date.valueOf("1990-05-15"),
        convertedAttributes[1],
        "Attribute 'birthdate' should be converted to Date '1990-05-15'");

    // Verify that parseStructMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /** Test the constructor with TIMESTAMP type elements. */
  @Test
  public void constructor_ShouldConvertStringToTimestampSuccessfully() throws SQLException {
    String metadata = "STRUCT<id:INT,created_at:TIMESTAMP>";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "1500");
    attributes.put("created_at", "2024-01-01 12:30:45");

    // Mock MetadataParser.parseStructMetadata to return a map of field types
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    typeMap.put("created_at", "TIMESTAMP");
    mockParseStructMetadata(metadata, typeMap);

    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);

    // Retrieve attributes
    Object[] convertedAttributes = databricksStruct.getAttributes();

    // Assertions
    assertNotNull(convertedAttributes, "Converted attributes should not be null");
    assertEquals(2, convertedAttributes.length, "Struct should have two attributes");
    assertEquals(
        1500, convertedAttributes[0], "Attribute 'id' should be converted to Integer 1500");
    assertEquals(
        Timestamp.valueOf("2024-01-01 12:30:45"),
        convertedAttributes[1],
        "Attribute 'created_at' should be converted to Timestamp");

    // Verify that parseStructMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /** Test the constructor with TIME type elements. */
  @Test
  public void constructor_ShouldConvertStringToTimeSuccessfully() throws SQLException {
    String metadata = "STRUCT<id:INT, login_time:TIME>";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "1600");
    attributes.put("login_time", "08:45:00");

    // Mock MetadataParser.parseStructMetadata to return a map of field types
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    typeMap.put("login_time", "TIME");
    mockParseStructMetadata(metadata, typeMap);

    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);

    // Retrieve attributes
    Object[] convertedAttributes = databricksStruct.getAttributes();

    // Assertions
    assertNotNull(convertedAttributes, "Converted attributes should not be null");
    assertEquals(2, convertedAttributes.length, "Struct should have two attributes");
    assertEquals(
        1600, convertedAttributes[0], "Attribute 'id' should be converted to Integer 1600");
    assertEquals(
        Time.valueOf("08:45:00"),
        convertedAttributes[1],
        "Attribute 'login_time' should be converted to Time '08:45:00'");

    // Verify that parseStructMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /** Test the getSQLTypeName() method. */
  @Test
  public void getSQLTypeName_ShouldReturnCorrectMetadata() throws SQLException {
    String metadata = "STRUCT<id:INT,name:STRING>";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "1800");
    attributes.put("name", "Irene");

    // Mock MetadataParser.parseStructMetadata to return a map of field types
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    typeMap.put("name", "STRING");
    mockParseStructMetadata(metadata, typeMap);

    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);

    String typeName = databricksStruct.getSQLTypeName();
    assertEquals(metadata, typeName, "getSQLTypeName() should return the original metadata");

    // Verify that parseStructMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /** Test the getAttributes() method. */
  @Test
  public void getAttributes_ShouldReturnConvertedAttributes() throws SQLException {
    String metadata = "STRUCT<id:INT,name:STRING,active:BOOLEAN>";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "1900");
    attributes.put("name", "Frank");
    attributes.put("active", "false");

    // Mock MetadataParser.parseStructMetadata to return a map of field types
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    typeMap.put("name", "STRING");
    typeMap.put("active", "BOOLEAN");
    mockParseStructMetadata(metadata, typeMap);

    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);

    Object[] convertedAttributes = databricksStruct.getAttributes();

    // Assertions
    assertNotNull(convertedAttributes, "Converted attributes should not be null");
    assertEquals(3, convertedAttributes.length, "Struct should have three attributes");
    assertEquals(
        1900, convertedAttributes[0], "Attribute 'id' should be converted to Integer 1900");
    assertEquals("Frank", convertedAttributes[1], "Attribute 'name' should be 'Frank'");
    assertEquals(false, convertedAttributes[2], "Attribute 'active' should be false");

    // Verify that parseStructMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /** Test the getAttributes(Map<String, Class<?>> map) method. */
  @Test
  public void getAttributes_WithTypeMap_ShouldReturnConvertedAttributes() throws SQLException {
    String metadata = "STRUCT<id:INT,name:STRING>";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "2000");
    attributes.put("name", "Jack");

    // Mock MetadataParser.parseStructMetadata to return a map of field types
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    typeMap.put("name", "STRING");
    mockParseStructMetadata(metadata, typeMap);

    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);

    // Create a type map (though in this implementation, it's not used)
    Map<String, Class<?>> typeMapParam = new HashMap<>();
    typeMapParam.put("id", Integer.class);
    typeMapParam.put("name", String.class);

    Object[] convertedAttributes = databricksStruct.getAttributes(typeMapParam);

    // Assertions
    assertNotNull(convertedAttributes, "Converted attributes should not be null");
    assertEquals(2, convertedAttributes.length, "Struct should have two attributes");
    assertEquals(
        2000, convertedAttributes[0], "Attribute 'id' should be converted to Integer 2000");
    assertEquals("Jack", convertedAttributes[1], "Attribute 'name' should be 'Jack'");

    // Verify that parseStructMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /** Test the constructor with null values in attributes. */
  @Test
  public void constructor_ShouldHandleNullValuesInAttributes() throws SQLException {
    String metadata = "STRUCT<id:INT,name:STRING,active:BOOLEAN>";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", null);
    attributes.put("name", "Henry");
    attributes.put("active", null);

    // Mock MetadataParser.parseStructMetadata to return a map of field types
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    typeMap.put("name", "STRING");
    typeMap.put("active", "BOOLEAN");
    mockParseStructMetadata(metadata, typeMap);

    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);

    // Retrieve attributes
    Object[] convertedAttributes = databricksStruct.getAttributes();

    // Assertions
    assertNotNull(convertedAttributes, "Converted attributes should not be null");
    assertEquals(3, convertedAttributes.length, "Struct should have three attributes");
    assertNull(convertedAttributes[0], "Attribute 'id' should be null");
    assertEquals("Henry", convertedAttributes[1], "Attribute 'name' should be 'Henry'");
    assertNull(convertedAttributes[2], "Attribute 'active' should be null");

    // Verify that parseStructMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /** Test the constructor with null values in attributes with complex field types. */
  @Test
  public void constructor_ShouldHandleNullValuesInComplexAttributes() throws SQLException {
    String metadata = "STRUCT<id:INT,map:STRUCT<value:STRING>,array:ARRAY<STRING>>";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", null);
    attributes.put("map", null);
    attributes.put("array", null);

    // Mock MetadataParser.parseStructMetadata to return a map of field types
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    typeMap.put("map", "STRUCT<value:STRING>");
    typeMap.put("array", "ARRAY<STRING>");
    mockParseStructMetadata(metadata, typeMap);

    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, metadata);

    // Retrieve attributes
    Object[] convertedAttributes = databricksStruct.getAttributes();

    // Assertions
    assertNotNull(convertedAttributes, "Converted attributes should not be null");
    assertEquals(3, convertedAttributes.length, "Struct should have three attributes");
    assertNull(convertedAttributes[0], "Attribute 'id' should be null");
    assertNull(convertedAttributes[1], "Attribute 'map' should be null");
    assertNull(convertedAttributes[2], "Attribute 'array' should be null");

    // Verify that parseStructMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
  }

  /*
   * Test the constructor with conversion failure in nested Struct. Expects
   * DatabricksDriverException.
   */
  @Test
  public void constructor_ShouldThrowException_WhenConversionFailsInNestedStruct()
      throws SQLException {
    String metadata = "STRUCT<id:INT, address:STRUCT<street:STRING, zipcode:INT>>";
    Map<String, Object> address = new HashMap<>();
    address.put("street", "789 Pine St");
    address.put("zipcode", "InvalidZip"); // Should be an Integer

    Map<String, Object> attributes = new HashMap<>();
    attributes.put("id", "1900");
    attributes.put("address", address);

    // Mock MetadataParser.parseStructMetadata for the outer struct
    Map<String, String> outerTypeMap = new LinkedHashMap<>();
    outerTypeMap.put("id", "INT");
    outerTypeMap.put("address", "STRUCT<street:STRING,zipcode:INT>");
    mockParseStructMetadata(metadata, outerTypeMap);

    // Mock MetadataParser.parseStructMetadata for the nested struct
    Map<String, String> innerTypeMap = new LinkedHashMap<>();
    innerTypeMap.put("street", "STRING");
    innerTypeMap.put("zipcode", "INT");
    mockParseStructMetadata("STRUCT<street:STRING,zipcode:INT>", innerTypeMap);

    // Expecting DatabricksDriverException due to conversion failure for 'zipcode'
    DatabricksDriverException exception =
        assertThrows(
            DatabricksDriverException.class,
            () -> {
              new DatabricksStruct(attributes, metadata);
            },
            "Constructor should throw DatabricksDriverException when conversion fails in nested struct");

    assertTrue(
        exception.getMessage().contains("Failed to convert value"),
        "Exception message should indicate conversion failure");

    // Verify that parseStructMetadata was called twice
    metadataParserMock.verify(() -> MetadataParser.parseStructMetadata(metadata), times(1));
    metadataParserMock.verify(
        () -> MetadataParser.parseStructMetadata("STRUCT<street:STRING,zipcode:INT>"), times(1));
  }

  @Test
  public void testStructToString_WithIntAndStringFields_ShouldProduceJsonLikeString()
      throws SQLException {
    // Arrange
    String structMetadata = "STRUCT<id:INT,name:STRING>";

    // Since the static mocking is already set up in this thread,
    // we directly tell the mock how to respond for parseStructMetadata(...).
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    typeMap.put("name", "STRING");

    // This metadataParserMock is presumably a static mock created in your @BeforeEach or a global
    // rule
    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(structMetadata))
        .thenReturn(typeMap);

    // Create attributes consistent with the metadata
    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("id", 123); // Matches INT
    attributes.put("name", "foo"); // Matches STRING

    // Act
    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, structMetadata);
    String actual = databricksStruct.toString();

    // Assert
    // Expect JSON-like output with unquoted int and quoted string: {"id":123,"name":"foo"}
    String expected = "{\"id\":123,\"name\":\"foo\"}";
    assertEquals(
        expected,
        actual,
        "Struct toString() must produce JSON-like output with int unquoted and string quoted");
  }

  @Test
  public void testStructToString_WithNestedArrayAndMap_ShouldProduceJsonLikeString()
      throws SQLException {
    // Arrange
    String structMetadata = "STRUCT<id:INT,fruits:ARRAY<STRING>,scores:MAP<STRING,INT>>";

    Map<String, String> structTypeMap = new LinkedHashMap<>();
    structTypeMap.put("id", "INT");
    structTypeMap.put("fruits", "ARRAY<STRING>");
    structTypeMap.put("scores", "MAP<STRING,INT>");
    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(structMetadata))
        .thenReturn(structTypeMap);

    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    metadataParserMock
        .when(() -> MetadataParser.parseMapMetadata("MAP<STRING,INT>"))
        .thenReturn("STRING,INT");

    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("id", 123);
    attributes.put("fruits", Arrays.asList("apple", "banana")); // array of strings
    Map<String, Integer> scoresMap = new LinkedHashMap<>();
    scoresMap.put("key1", 10);
    scoresMap.put("key2", 20);
    attributes.put("scores", scoresMap);

    // Act
    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, structMetadata);
    String actual = databricksStruct.toString();

    // Assert
    String expected =
        "{\"id\":123,\"fruits\":[\"apple\",\"banana\"],\"scores\":{\"key1\":10,\"key2\":20}}";
    assertEquals(
        expected,
        actual,
        "Struct toString() must produce JSON-like output with int unquoted, array of quoted strings, and map with string keys/ int values");
  }

  /**
   * Test VARIANT fields with JsonNode objects to verify double-escaping fix. This test ensures that
   * VARIANT fields containing JsonNode objects are serialized correctly without double-escaping,
   * while preserving JSON structure.
   */
  @Test
  public void constructor_ShouldHandleVariantFieldsCorrectly() throws Exception {
    // Arrange
    String structMetadata =
        "STRUCT<id:INT,simple_str:STRING,variant_obj:VARIANT,variant_arr:VARIANT,variant_null:VARIANT,variant_primitive:VARIANT>";

    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("id", "INT");
    typeMap.put("simple_str", "STRING");
    typeMap.put("variant_obj", "VARIANT");
    typeMap.put("variant_arr", "VARIANT");
    typeMap.put("variant_null", "VARIANT");
    typeMap.put("variant_primitive", "VARIANT");
    mockParseStructMetadata(structMetadata, typeMap);

    // Create JsonNode objects representing VARIANT data (as would come from ComplexDataTypeParser)
    ObjectMapper mapper = new ObjectMapper();
    JsonNode variantObj = mapper.readTree("{\"nested\":{\"key\":\"value\",\"number\":42}}");
    JsonNode variantArr = mapper.readTree("[{\"item\":\"first\"},{\"item\":\"second\"}]");
    JsonNode variantNull = mapper.readTree("null");
    JsonNode variantPrimitive = mapper.readTree("\"simple_string\"");

    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("id", 123);
    attributes.put("simple_str", "regular_string");
    attributes.put("variant_obj", variantObj); // JsonNode object
    attributes.put("variant_arr", variantArr); // JsonNode array
    attributes.put("variant_null", variantNull); // JsonNode null
    attributes.put("variant_primitive", variantPrimitive); // JsonNode primitive

    // Act
    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, structMetadata);
    String actual = databricksStruct.toString();

    // Verify that regular fields are correctly formatted
    assertTrue(actual.contains("\"id\":123"), "ID field should be unquoted integer");
    assertTrue(
        actual.contains("\"simple_str\":\"regular_string\""), "String field should be quoted");

    // Verify that VARIANT fields preserve JSON structure without double-escaping
    assertTrue(
        actual.contains("\"variant_obj\":{\"nested\":{\"key\":\"value\",\"number\":42}}"),
        "VARIANT object should preserve JSON structure without double-escaping");
    assertTrue(
        actual.contains("\"variant_arr\":[{\"item\":\"first\"},{\"item\":\"second\"}]"),
        "VARIANT array should preserve JSON structure without double-escaping");
    assertTrue(
        actual.contains("\"variant_null\":null"), "VARIANT null should be serialized as null");
    assertTrue(
        actual.contains("\"variant_primitive\":\"simple_string\""),
        "VARIANT primitive should preserve quotes for string values");

    // Verify NO double-escaping patterns exist
    assertFalse(actual.contains("\\\"{"), "Should not contain double-escaped opening braces");
    assertFalse(actual.contains("}\\\""), "Should not contain double-escaped closing braces");
    assertFalse(actual.contains("\\\"["), "Should not contain double-escaped opening brackets");
    assertFalse(actual.contains("]\\\""), "Should not contain double-escaped closing brackets");

    // Verify the complete expected structure
    String expectedStructure =
        "{\"id\":123,"
            + "\"simple_str\":\"regular_string\","
            + "\"variant_obj\":{\"nested\":{\"key\":\"value\",\"number\":42}},"
            + "\"variant_arr\":[{\"item\":\"first\"},{\"item\":\"second\"}],"
            + "\"variant_null\":null,"
            + "\"variant_primitive\":\"simple_string\"}";

    assertEquals(
        expectedStructure,
        actual,
        "Complete struct should match expected format with properly serialized VARIANT fields");
  }

  /**
   * Test VARIANT field conversion in convertSimpleValue to ensure JsonNode objects are preserved.
   */
  @Test
  public void convertSimpleValue_ShouldPreserveJsonNodeForVariant() throws Exception {
    // Arrange
    String structMetadata = "STRUCT<variant_field:VARIANT>";
    Map<String, String> typeMap = new LinkedHashMap<>();
    typeMap.put("variant_field", "VARIANT");
    mockParseStructMetadata(structMetadata, typeMap);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"test\":\"data\"}");

    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("variant_field", jsonNode);

    // Act
    DatabricksStruct databricksStruct = new DatabricksStruct(attributes, structMetadata);
    Object[] convertedAttributes = databricksStruct.getAttributes();

    // Assert
    assertEquals(1, convertedAttributes.length, "Should have one attribute");
    assertInstanceOf(
        JsonNode.class,
        convertedAttributes[0],
        "VARIANT field should preserve JsonNode object in attributes array");

    JsonNode preservedNode = (JsonNode) convertedAttributes[0];
    assertEquals(
        "{\"test\":\"data\"}",
        preservedNode.toString(),
        "JsonNode content should be preserved exactly");
  }

  @Test
  public void testToStringWithTimestamp() throws SQLException {
    // Test that timestamps in structs are properly quoted
    java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2024-01-01 12:30:45.123");
    String metadata = "STRUCT<id:INT,created_at:TIMESTAMP>";

    Map<String, Object> fieldTypesMap = new LinkedHashMap<>();
    fieldTypesMap.put("id", "INT");
    fieldTypesMap.put("created_at", "TIMESTAMP");

    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(metadata))
        .thenReturn(fieldTypesMap);

    Map<String, Object> inputMap = new LinkedHashMap<>();
    inputMap.put("id", 123);
    inputMap.put("created_at", timestamp);

    DatabricksStruct struct = new DatabricksStruct(inputMap, metadata);
    String actual = struct.toString();

    String expected = "{\"id\":123,\"created_at\":\"2024-01-01 12:30:45.123\"}";
    assertEquals(expected, actual, "DatabricksStruct.toString() should quote timestamp fields");
  }

  @Test
  public void testToStringWithDate() throws SQLException {
    // Test that dates in structs are properly quoted
    java.sql.Date date = java.sql.Date.valueOf("2024-01-01");
    String metadata = "STRUCT<id:INT,event_date:DATE>";

    Map<String, Object> fieldTypesMap = new LinkedHashMap<>();
    fieldTypesMap.put("id", "INT");
    fieldTypesMap.put("event_date", "DATE");

    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(metadata))
        .thenReturn(fieldTypesMap);

    Map<String, Object> inputMap = new LinkedHashMap<>();
    inputMap.put("id", 456);
    inputMap.put("event_date", date);

    DatabricksStruct struct = new DatabricksStruct(inputMap, metadata);
    String actual = struct.toString();

    String expected = "{\"id\":456,\"event_date\":\"2024-01-01\"}";
    assertEquals(expected, actual, "DatabricksStruct.toString() should quote date fields");
  }

  @Test
  public void testToStringProducesValidJsonWithSpecialCharacters() throws SQLException {
    // Test that toString() produces VALID JSON when strings contain special characters
    String metadata = "STRUCT<id:INT,message:STRING,path:STRING>";

    Map<String, Object> fieldTypesMap = new LinkedHashMap<>();
    fieldTypesMap.put("id", "INT");
    fieldTypesMap.put("message", "STRING");
    fieldTypesMap.put("path", "STRING");

    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(metadata))
        .thenReturn(fieldTypesMap);

    Map<String, Object> inputMap = new LinkedHashMap<>();
    inputMap.put("id", 1);
    inputMap.put("message", "She said \"hello\"");
    inputMap.put("path", "C:\\Users\\file.txt");

    DatabricksStruct struct = new DatabricksStruct(inputMap, metadata);
    String actual = struct.toString();

    System.out.println("Generated JSON: " + actual);

    try {
      new ObjectMapper().readTree(actual);
      // Success! The JSON is valid
      System.out.println("JSON parsing succeeded - output is valid JSON");
    } catch (Exception e) {
      fail("JSON parsing failed. JSON was invalid: " + actual + "\nError: " + e.getMessage());
    }
  }
}
