package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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

/** Comprehensive and type-safe test cases for DatabricksMap. */
public class DatabricksMapTest {

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
   * Helper method to mock MetadataParser.parseMapMetadata based on the map metadata string.
   *
   * @param mapMetadata the metadata string describing the map
   * @param keyType the expected key type
   * @param valueType the expected value type
   */
  private void mockParseMapMetadata(String mapMetadata, String keyType, String valueType) {
    String combined = keyType + "," + valueType;
    metadataParserMock
        .when(() -> MetadataParser.parseMapMetadata(mapMetadata))
        .thenReturn(combined);
  }

  /** Test the constructor with valid simple types. */
  @Test
  public void testConstructor_WithSimpleTypes_ShouldConvertCorrectly() throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();
    originalMap.put("key1", "value1");
    originalMap.put("key2", "value2");

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions
    assertNotNull(databricksMap, "DatabricksMap instance should not be null");
    assertEquals(2, databricksMap.size(), "DatabricksMap should have two entries");
    assertEquals("value1", databricksMap.get("key1"), "Value for 'key1' should be 'value1'");
    assertEquals("value2", databricksMap.get("key2"), "Value for 'key2' should be 'value2'");

    // Verify that parseMapMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the constructor with different key and value types. */
  @Test
  public void testConstructor_WithIntBooleanTypes_ShouldConvertCorrectly() throws SQLException {
    String metadata = "MAP<INT, BOOLEAN>";
    Map<Integer, Boolean> originalMap = new HashMap<>();
    originalMap.put(1, true);
    originalMap.put(2, false);

    // Mock MetadataParser.parseMapMetadata to return "INT,BOOLEAN"
    mockParseMapMetadata(metadata, "INT", "BOOLEAN");

    DatabricksMap<Integer, Boolean> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions
    assertNotNull(databricksMap, "DatabricksMap instance should not be null");
    assertEquals(2, databricksMap.size(), "DatabricksMap should have two entries");
    assertEquals(true, databricksMap.get(1), "Value for key 1 should be true");
    assertEquals(false, databricksMap.get(2), "Value for key 2 should be false");

    // Verify that parseMapMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the constructor with nested Struct values. */
  @Test
  public void testConstructor_WithNestedStructValues_ShouldHandleProperly() throws SQLException {
    String metadata = "MAP<STRING, STRUCT<id:INT,name:STRING>>";
    Map<String, DatabricksStruct> originalMap = new HashMap<>();

    // Mock MetadataParser.parseStructMetadata for the nested struct
    Map<String, String> structTypeMap = new LinkedHashMap<>();
    structTypeMap.put("id", "INT");
    structTypeMap.put("name", "STRING");
    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata("STRUCT<id:INT,name:STRING>"))
        .thenReturn(structTypeMap);

    // Create DatabricksStruct instances with correct constructor parameters
    Map<String, Object> structFields1 = new HashMap<>();
    structFields1.put("id", 10);
    structFields1.put("name", "Alice");
    DatabricksStruct struct1 = new DatabricksStruct(structFields1, "STRUCT<id:INT,name:STRING>");

    Map<String, Object> structFields2 = new HashMap<>();
    structFields2.put("id", 20);
    structFields2.put("name", "Bob");
    DatabricksStruct struct2 = new DatabricksStruct(structFields2, "STRUCT<id:INT,name:STRING>");

    originalMap.put("user1", struct1);
    originalMap.put("user2", struct2);

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRUCT<id:INT,name:STRING>"
    mockParseMapMetadata(metadata, "STRING", "STRUCT<id:INT,name:STRING>");

    DatabricksMap<String, DatabricksStruct> databricksMap =
        new DatabricksMap<>(originalMap, metadata);

    // Assertions
    assertNotNull(databricksMap, "DatabricksMap instance should not be null");
    assertEquals(2, databricksMap.size(), "DatabricksMap should have two entries");

    DatabricksStruct user1 = databricksMap.get("user1");
    assertNotNull(user1, "User1 struct should not be null");
    Object[] user1Attributes = user1.getAttributes();
    assertEquals(2, user1Attributes.length, "User1 struct should have two attributes");
    assertEquals(10, user1Attributes[0], "User1 id should be 10");
    assertEquals("Alice", user1Attributes[1], "User1 name should be Alice");

    DatabricksStruct user2 = databricksMap.get("user2");
    assertNotNull(user2, "User2 struct should not be null");
    Object[] user2Attributes = user2.getAttributes();
    assertEquals(2, user2Attributes.length, "User2 struct should have two attributes");
    assertEquals(20, user2Attributes[0], "User2 id should be 20");
    assertEquals("Bob", user2Attributes[1], "User2 name should be Bob");

    // Verify that parseMapMetadata and parseStructMetadata were called appropriately
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
    metadataParserMock.verify(
        () -> MetadataParser.parseStructMetadata("STRUCT<id:INT,name:STRING>"), times(2));
  }

  /** Test the constructor with nested Array values. */
  @Test
  public void testConstructor_WithNestedArrayValues_ShouldHandleProperly() throws SQLException {
    String metadata = "MAP<STRING, ARRAY<STRING>>";
    Map<String, DatabricksArray> originalMap = new HashMap<>();

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    // Create DatabricksArray instances with correct constructor parameters
    List<Object> list1 = Arrays.asList("apple", "banana");
    DatabricksArray array1 = new DatabricksArray(list1, "ARRAY<STRING>");

    List<Object> list2 = Arrays.asList("cherry", "date");
    DatabricksArray array2 = new DatabricksArray(list2, "ARRAY<STRING>");

    originalMap.put("fruits1", array1);
    originalMap.put("fruits2", array2);

    // Mock MetadataParser.parseMapMetadata to return "STRING,ARRAY<STRING>"
    mockParseMapMetadata(metadata, "STRING", "ARRAY<STRING>");

    DatabricksMap<String, DatabricksArray> databricksMap =
        new DatabricksMap<>(originalMap, metadata);

    // Assertions
    assertNotNull(databricksMap, "DatabricksMap instance should not be null");
    assertEquals(2, databricksMap.size(), "DatabricksMap should have two entries");

    DatabricksArray fruits1 = databricksMap.get("fruits1");
    assertNotNull(fruits1, "Fruits1 array should not be null");
    Object[] fruits1Elements = (Object[]) fruits1.getArray();
    assertEquals(2, fruits1Elements.length, "Fruits1 array should have two elements");
    assertEquals("apple", fruits1Elements[0], "First element should be 'apple'");
    assertEquals("banana", fruits1Elements[1], "Second element should be 'banana'");

    DatabricksArray fruits2 = databricksMap.get("fruits2");
    assertNotNull(fruits2, "Fruits2 array should not be null");
    Object[] fruits2Elements = (Object[]) fruits2.getArray();
    assertEquals(2, fruits2Elements.length, "Fruits2 array should have two elements");
    assertEquals("cherry", fruits2Elements[0], "First element should be 'cherry'");
    assertEquals("date", fruits2Elements[1], "Second element should be 'date'");

    // Verify that parseMapMetadata and parseArrayMetadata were called appropriately
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"), times(2));
  }

  /**
   * Test the constructor with invalid key type (providing a non-convertible key). Expects
   * IllegalArgumentException.
   */
  @Test
  public void testConstructor_WithInvalidKeyType_ShouldThrowException() throws SQLException {
    String metadata = "MAP<INT, STRING>";
    Map<Object, String> invalidMap = new HashMap<>();
    invalidMap.put("one", "value1"); // String key instead of Integer
    invalidMap.put("two", "value2"); // String key instead of Integer

    // Mock MetadataParser.parseMapMetadata to return "INT,STRING"
    mockParseMapMetadata(metadata, "INT", "STRING");

    // Expecting IllegalArgumentException due to key conversion failure
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              // Cast to Map<Integer, String> using raw types and suppression
              @SuppressWarnings("unchecked")
              Map<Integer, String> castedMap = (Map<Integer, String>) (Map<?, ?>) invalidMap;
              DatabricksMap<Integer, String> databricksMap =
                  new DatabricksMap<>(castedMap, metadata);
            },
            "Constructor should throw IllegalArgumentException when key conversion fails");

    assertTrue(
        exception.getMessage().contains("Invalid metadata or map structure"),
        "Exception message should indicate conversion failure for key");

    // Verify that parseMapMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the constructor with empty original map. */
  @Test
  public void testConstructor_WithEmptyOriginalMap_ShouldHandleProperly() throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions
    assertNotNull(databricksMap, "DatabricksMap instance should not be null");
    assertTrue(databricksMap.isEmpty(), "DatabricksMap should be empty");

    // Verify that parseMapMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the constructor with null values in the original map. */
  @Test
  public void testConstructor_WithNullValuesInOriginalMap_ShouldHandleProperly()
      throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();
    originalMap.put("key1", null);
    originalMap.put("key2", "value2");

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions
    assertNotNull(databricksMap, "DatabricksMap instance should not be null");
    assertEquals(2, databricksMap.size(), "DatabricksMap should have two entries");
    assertNull(databricksMap.get("key1"), "Value for 'key1' should be null");
    assertEquals("value2", databricksMap.get("key2"), "Value for 'key2' should be 'value2'");

    // Verify that parseMapMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the size() method. */
  @Test
  public void testMapInterfaceMethods_Size_ShouldReturnCorrectSize() throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();
    originalMap.put("key1", "value1");
    originalMap.put("key2", "value2");
    originalMap.put("key3", "value3");

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertion
    assertEquals(3, databricksMap.size(), "Map size should be 3");

    // Verify that parseMapMetadata was called once
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the isEmpty() method. */
  @Test
  public void testMapInterfaceMethods_IsEmpty_ShouldReturnCorrectStatus() throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertion before adding entries
    assertTrue(databricksMap.isEmpty(), "Map should be empty before adding entries");

    // Add an entry and verify
    databricksMap.put("key1", "value1");
    assertFalse(databricksMap.isEmpty(), "Map should not be empty after adding an entry");

    // Verify that parseMapMetadata was called once
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the containsKey() method. */
  @Test
  public void testMapInterfaceMethods_ContainsKey_ShouldReturnCorrectStatus() throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();
    originalMap.put("key1", "value1");

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions
    assertTrue(databricksMap.containsKey("key1"), "Map should contain key 'key1'");
    assertFalse(databricksMap.containsKey("key2"), "Map should not contain key 'key2'");

    // Verify that parseMapMetadata was called once
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the containsValue() method. */
  @Test
  public void testMapInterfaceMethods_ContainsValue_ShouldReturnCorrectStatus()
      throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();
    originalMap.put("key1", "value1");

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions
    assertTrue(databricksMap.containsValue("value1"), "Map should contain value 'value1'");
    assertFalse(databricksMap.containsValue("value2"), "Map should not contain value 'value2'");

    // Verify that parseMapMetadata was called once
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the get() method. */
  @Test
  public void testMapInterfaceMethods_Get_ShouldReturnCorrectValue() throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();
    originalMap.put("key1", "value1");
    originalMap.put("key2", "value2");

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions
    assertEquals("value1", databricksMap.get("key1"), "Value for 'key1' should be 'value1'");
    assertNull(databricksMap.get("key3"), "Value for non-existent key 'key3' should be null");

    // Verify that parseMapMetadata was called once
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the put() method. */
  @Test
  public void testMapInterfaceMethods_Put_ShouldAddOrUpdateEntries() throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions before putting
    assertTrue(databricksMap.isEmpty(), "Map should be empty before putting entries");

    // Put a new entry
    assertNull(databricksMap.put("key1", "value1"), "Previous value for 'key1' should be null");
    assertEquals("value1", databricksMap.get("key1"), "Value for 'key1' should be 'value1'");
    assertEquals(1, databricksMap.size(), "Map size should be 1 after adding one entry");

    // Put another entry
    assertNull(databricksMap.put("key2", "value2"), "Previous value for 'key2' should be null");
    assertEquals("value2", databricksMap.get("key2"), "Value for 'key2' should be 'value2'");
    assertEquals(2, databricksMap.size(), "Map size should be 2 after adding two entries");

    // Update existing entry
    assertEquals(
        "value1",
        databricksMap.put("key1", "newValue1"),
        "Previous value for 'key1' should be 'value1'");
    assertEquals("newValue1", databricksMap.get("key1"), "Value for 'key1' should be 'newValue1'");
    assertEquals(2, databricksMap.size(), "Map size should remain 2 after updating an entry");

    // Verify that parseMapMetadata was called once
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the remove() method. */
  @Test
  public void testMapInterfaceMethods_Remove_ShouldRemoveEntries() throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();
    originalMap.put("key1", "value1");
    originalMap.put("key2", "value2");

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions before removal
    assertEquals(2, databricksMap.size(), "Map should have two entries before removal");

    // Remove an existing entry
    assertEquals(
        "value1", databricksMap.remove("key1"), "Removed value for 'key1' should be 'value1'");
    assertNull(databricksMap.get("key1"), "Value for 'key1' should be null after removal");
    assertEquals(1, databricksMap.size(), "Map size should be 1 after removal");

    // Attempt to remove a non-existent entry
    assertNull(databricksMap.remove("key3"), "Removing non-existent key 'key3' should return null");
    assertEquals(
        1,
        databricksMap.size(),
        "Map size should remain 1 after attempting to remove non-existent key");

    // Verify that parseMapMetadata was called once
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the putAll() method. */
  @Test
  public void testMapInterfaceMethods_PutAll_ShouldAddMultipleEntries() throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();
    originalMap.put("key1", "value1");

    Map<String, String> newEntries = new HashMap<>();
    newEntries.put("key2", "value2");
    newEntries.put("key3", "value3");

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions before putAll
    assertEquals(1, databricksMap.size(), "Map should have one entry before putAll");

    // Perform putAll
    databricksMap.putAll(newEntries);

    // Assertions after putAll
    assertEquals(3, databricksMap.size(), "Map should have three entries after putAll");
    assertEquals("value1", databricksMap.get("key1"), "Value for 'key1' should be 'value1'");
    assertEquals("value2", databricksMap.get("key2"), "Value for 'key2' should be 'value2'");
    assertEquals("value3", databricksMap.get("key3"), "Value for 'key3' should be 'value3'");

    // Verify that parseMapMetadata was called once
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the clear() method. */
  @Test
  public void testMapInterfaceMethods_Clear_ShouldRemoveAllEntries() throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();
    originalMap.put("key1", "value1");
    originalMap.put("key2", "value2");

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions before clear
    assertFalse(databricksMap.isEmpty(), "Map should not be empty before clear");
    assertEquals(2, databricksMap.size(), "Map size should be 2 before clear");

    // Clear the map
    databricksMap.clear();

    // Assertions after clear
    assertTrue(databricksMap.isEmpty(), "Map should be empty after clear");
    assertEquals(0, databricksMap.size(), "Map size should be 0 after clear");

    // Verify that parseMapMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the keySet() method. */
  @Test
  public void testMapInterfaceMethods_KeySet_ShouldReturnCorrectKeys() throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();
    originalMap.put("key1", "value1");
    originalMap.put("key2", "value2");

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    Set<String> keys = databricksMap.keySet();
    assertNotNull(keys, "Key set should not be null");
    assertEquals(2, keys.size(), "Key set should contain two keys");
    assertTrue(keys.contains("key1"), "Key set should contain 'key1'");
    assertTrue(keys.contains("key2"), "Key set should contain 'key2'");

    // Verify that parseMapMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the values() method. */
  @Test
  public void testMapInterfaceMethods_Values_ShouldReturnCorrectValues() throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();
    originalMap.put("key1", "value1");
    originalMap.put("key2", "value2");

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    Collection<String> values = databricksMap.values();
    assertNotNull(values, "Values collection should not be null");
    assertEquals(2, values.size(), "Values collection should contain two values");
    assertTrue(values.contains("value1"), "Values collection should contain 'value1'");
    assertTrue(values.contains("value2"), "Values collection should contain 'value2'");

    // Verify that parseMapMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the entrySet() method. */
  @Test
  public void testMapInterfaceMethods_EntrySet_ShouldReturnCorrectEntries() throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();
    originalMap.put("key1", "value1");
    originalMap.put("key2", "value2");

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    Set<Map.Entry<String, String>> entries = databricksMap.entrySet();
    assertNotNull(entries, "Entry set should not be null");
    assertEquals(2, entries.size(), "Entry set should contain two entries");

    for (Map.Entry<String, String> entry : entries) {
      assertTrue(
          originalMap.containsKey(entry.getKey()),
          "Original map should contain key: " + entry.getKey());
      assertEquals(
          originalMap.get(entry.getKey()),
          entry.getValue(),
          "Value should match for key: " + entry.getKey());
    }

    // Verify that parseMapMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the constructor with binary type values. */
  @Test
  public void testConstructor_WithBinaryValues_ShouldHandleCorrectly() throws SQLException {
    String metadata = "MAP<STRING, BINARY>";
    Map<String, byte[]> originalMap = new HashMap<>();
    originalMap.put("data1", "binaryData1".getBytes());
    originalMap.put("data2", "binaryData2".getBytes());

    // Mock MetadataParser.parseMapMetadata to return "STRING,BINARY"
    mockParseMapMetadata(metadata, "STRING", "BINARY");

    DatabricksMap<String, byte[]> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions
    assertNotNull(databricksMap, "DatabricksMap instance should not be null");
    assertEquals(2, databricksMap.size(), "DatabricksMap should have two entries");
    assertArrayEquals(
        "binaryData1".getBytes(),
        databricksMap.get("data1"),
        "Value for 'data1' should match the binary data");
    assertArrayEquals(
        "binaryData2".getBytes(),
        databricksMap.get("data2"),
        "Value for 'data2' should match the binary data");

    // Verify that parseMapMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the constructor with DATE type values. */
  @Test
  public void testConstructor_WithDateValues_ShouldConvertSuccessfully() throws SQLException {
    String metadata = "MAP<STRING, DATE>";
    Map<String, Date> originalMap = new HashMap<>();
    originalMap.put("birthday1", Date.valueOf("1990-01-01"));
    originalMap.put("birthday2", Date.valueOf("1985-12-31"));

    // Mock MetadataParser.parseMapMetadata to return "STRING,DATE"
    mockParseMapMetadata(metadata, "STRING", "DATE");

    DatabricksMap<String, Date> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions
    assertNotNull(databricksMap, "DatabricksMap instance should not be null");
    assertEquals(2, databricksMap.size(), "DatabricksMap should have two entries");
    assertEquals(
        Date.valueOf("1990-01-01"),
        databricksMap.get("birthday1"),
        "Value for 'birthday1' should be Date '1990-01-01'");
    assertEquals(
        Date.valueOf("1985-12-31"),
        databricksMap.get("birthday2"),
        "Value for 'birthday2' should be Date '1985-12-31'");

    // Verify that parseMapMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the constructor with TIMESTAMP type values. */
  @Test
  public void testConstructor_WithTimestampValues_ShouldConvertSuccessfully() throws SQLException {
    String metadata = "MAP<STRING, TIMESTAMP>";
    Map<String, Timestamp> originalMap = new HashMap<>();
    originalMap.put("event1", Timestamp.valueOf("2024-01-01 12:00:00"));
    originalMap.put("event2", Timestamp.valueOf("2024-06-15 18:30:45"));

    // Mock MetadataParser.parseMapMetadata to return "STRING,TIMESTAMP"
    mockParseMapMetadata(metadata, "STRING", "TIMESTAMP");

    DatabricksMap<String, Timestamp> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions
    assertNotNull(databricksMap, "DatabricksMap instance should not be null");
    assertEquals(2, databricksMap.size(), "DatabricksMap should have two entries");
    assertEquals(
        Timestamp.valueOf("2024-01-01 12:00:00"),
        databricksMap.get("event1"),
        "Value for 'event1' should be Timestamp '2024-01-01 12:00:00'");
    assertEquals(
        Timestamp.valueOf("2024-06-15 18:30:45"),
        databricksMap.get("event2"),
        "Value for 'event2' should be Timestamp '2024-06-15 18:30:45'");

    // Verify that parseMapMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the constructor with TIME type values. */
  @Test
  public void testConstructor_WithTimeValues_ShouldConvertSuccessfully() throws SQLException {
    String metadata = "MAP<STRING, TIME>";
    Map<String, Time> originalMap = new HashMap<>();
    originalMap.put("login1", Time.valueOf("08:30:00"));
    originalMap.put("login2", Time.valueOf("17:45:30"));

    // Mock MetadataParser.parseMapMetadata to return "STRING,TIME"
    mockParseMapMetadata(metadata, "STRING", "TIME");

    DatabricksMap<String, Time> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Assertions
    assertNotNull(databricksMap, "DatabricksMap instance should not be null");
    assertEquals(2, databricksMap.size(), "DatabricksMap should have two entries");
    assertEquals(
        Time.valueOf("08:30:00"),
        databricksMap.get("login1"),
        "Value for 'login1' should be Time '08:30:00'");
    assertEquals(
        Time.valueOf("17:45:30"),
        databricksMap.get("login2"),
        "Value for 'login2' should be Time '17:45:30'");

    // Verify that parseMapMetadata was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
  }

  /** Test the constructor with nested Map values. */
  @Test
  public void testConstructor_WithNestedMapValues_ShouldHandleProperly() throws SQLException {
    String metadata = "MAP<STRING, MAP<STRING, STRING>>";
    Map<String, DatabricksMap<String, String>> originalMap = new HashMap<>();

    // Mock MetadataParser.parseMapMetadata to return "STRING,MAP<STRING,STRING>"
    mockParseMapMetadata(metadata, "STRING", "MAP<STRING,STRING>");

    // Mock MetadataParser.parseMapMetadata for the nested maps
    metadataParserMock
        .when(() -> MetadataParser.parseMapMetadata("MAP<STRING,STRING>"))
        .thenReturn("STRING,STRING");

    // Create nested DatabricksMap instances with correct constructor parameters
    Map<String, String> innerMap1Data = new HashMap<>();
    innerMap1Data.put("setting1", "on");
    innerMap1Data.put("setting2", "off");
    DatabricksMap<String, String> innerMap1 =
        new DatabricksMap<>(innerMap1Data, "MAP<STRING,STRING>");

    Map<String, String> innerMap2Data = new HashMap<>();
    innerMap2Data.put("volume", "high");
    innerMap2Data.put("brightness", "medium");
    DatabricksMap<String, String> innerMap2 =
        new DatabricksMap<>(innerMap2Data, "MAP<STRING,STRING>");

    originalMap.put("device1", innerMap1);
    originalMap.put("device2", innerMap2);

    DatabricksMap<String, DatabricksMap<String, String>> databricksMap =
        new DatabricksMap<>(originalMap, metadata);

    // Assertions
    assertNotNull(databricksMap, "DatabricksMap instance should not be null");
    assertEquals(2, databricksMap.size(), "DatabricksMap should have two entries");

    DatabricksMap<String, String> device1 = databricksMap.get("device1");
    assertNotNull(device1, "Device1 map should not be null");
    assertEquals(2, device1.size(), "Device1 map should have two entries");
    assertEquals("on", device1.get("setting1"), "Device1 setting1 should be 'on'");
    assertEquals("off", device1.get("setting2"), "Device1 setting2 should be 'off'");

    DatabricksMap<String, String> device2 = databricksMap.get("device2");
    assertNotNull(device2, "Device2 map should not be null");
    assertEquals(2, device2.size(), "Device2 map should have two entries");
    assertEquals("high", device2.get("volume"), "Device2 volume should be 'high'");
    assertEquals("medium", device2.get("brightness"), "Device2 brightness should be 'medium'");

    // Verify that parseMapMetadata was called appropriately
    metadataParserMock.verify(() -> MetadataParser.parseMapMetadata(metadata), times(1));
    metadataParserMock.verify(
        () -> MetadataParser.parseMapMetadata("MAP<STRING,STRING>"),
        times(4)); // Called for each nested map
  }

  /**
   * Test the constructor with concurrent modifications. Note: Assuming that DatabricksMap copies
   * the original map during construction, concurrent modifications after construction should not
   * affect the DatabricksMap.
   */
  @Test
  public void testConstructor_WithConcurrentModifications_ShouldRemainImmutable()
      throws SQLException {
    String metadata = "MAP<STRING, STRING>";
    Map<String, String> originalMap = new HashMap<>();
    originalMap.put("key1", "value1");

    // Mock MetadataParser.parseMapMetadata to return "STRING,STRING"
    mockParseMapMetadata(metadata, "STRING", "STRING");

    DatabricksMap<String, String> databricksMap = new DatabricksMap<>(originalMap, metadata);

    // Modify the original map after DatabricksMap construction
    originalMap.put("key2", "value2");
    originalMap.remove("key1");

    // Assertions to ensure DatabricksMap is unaffected
    assertEquals(
        1,
        databricksMap.size(),
        "DatabricksMap size should remain 1 after original map modification");
    assertEquals(
        "value1",
        databricksMap.get("key1"),
        "DatabricksMap should still contain 'key1' with 'value1'");
    assertFalse(databricksMap.containsKey("key2"), "DatabricksMap should not contain 'key2'");
  }

  @Test
  public void testMapToString_WithIntKeyAndStringValue_ShouldProduceJsonLikeString()
      throws SQLException {
    // Arrange
    String metadata = "MAP<INT, STRING>";
    // Mock MetadataParser to return "INT,STRING" for the map metadata
    mockParseMapMetadata(metadata, "INT", "STRING");

    // Use LinkedHashMap to maintain insertion order, so we can predict the toString() output
    Map<Integer, String> originalMap = new LinkedHashMap<>();
    originalMap.put(1, "one");
    originalMap.put(2, "two");

    // Act
    DatabricksMap<Integer, String> databricksMap = new DatabricksMap<>(originalMap, metadata);
    String result = databricksMap.toString();

    // Assert
    // The toString() output should be {1:"one",2:"two"}
    String expected = "{1:\"one\",2:\"two\"}";
    assertEquals(
        expected,
        result,
        "toString() should produce JSON-like output with INT keys unquoted and STRING values quoted");
  }

  @Test
  public void testMapToString_WithStringKeyAndNestedStructArrayValue_ShouldProduceJsonLikeString()
      throws SQLException {
    // Arrange
    String metadata = "MAP<STRING, STRUCT<age:INT, tags:ARRAY<STRING>>>";

    mockParseMapMetadata(metadata, "STRING", "STRUCT<age:INT,tags:ARRAY<STRING>>");

    Map<String, String> structTypeMap = new LinkedHashMap<>();
    structTypeMap.put("age", "INT");
    structTypeMap.put("tags", "ARRAY<STRING>");
    when(MetadataParser.parseStructMetadata("STRUCT<age:INT,tags:ARRAY<STRING>>"))
        .thenReturn(structTypeMap);

    when(MetadataParser.parseArrayMetadata("ARRAY<STRING>")).thenReturn("STRING");

    Map<String, Object> nestedStruct1 = new LinkedHashMap<>();
    nestedStruct1.put("age", 30);
    nestedStruct1.put("tags", Arrays.asList("red", "green", "blue"));

    Map<String, Object> nestedStruct2 = new LinkedHashMap<>();
    nestedStruct2.put("age", 40);
    nestedStruct2.put("tags", Arrays.asList("xyz"));

    Map<String, Map<String, Object>> originalMap = new LinkedHashMap<>();
    originalMap.put("person1", nestedStruct1);
    originalMap.put("person2", nestedStruct2);

    // Act
    DatabricksMap<String, Map<String, Object>> databricksMap =
        new DatabricksMap<>(originalMap, metadata);

    String result = databricksMap.toString();

    // Assert
    String expected =
        "{\"person1\":{\"age\":30,\"tags\":[\"red\",\"green\",\"blue\"]},\"person2\":{\"age\":40,\"tags\":[\"xyz\"]}}";

    assertEquals(
        expected,
        result,
        "toString() should produce JSON-like output with string keys, nested struct with int field, and array of strings");
  }
}
