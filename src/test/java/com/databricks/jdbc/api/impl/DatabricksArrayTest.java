package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Comprehensive and non-redundant test cases for DatabricksArray. */
public class DatabricksArrayTest {

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
   */
  private void mockParseStructMetadata(String structMetadata, Map<String, Object> returnType) {
    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(structMetadata))
        .thenReturn(returnType);
  }

  /** Test the constructor with a valid list of simple STRING elements. */
  @Test
  public void constructor_ShouldConvertStringElementsCorrectly() throws SQLException {
    String metadata = "ARRAY<STRING>";
    List<Object> originalList = Arrays.asList("apple", "banana", "cherry");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Retrieve the array
    Object arrayObj = databricksArray.getArray();
    assertNotNull(arrayObj, "getArray() should not return null");
    assertTrue(arrayObj instanceof Object[], "getArray() should return an Object[]");
    Object[] convertedElements = (Object[]) arrayObj;

    // Assertions
    assertEquals(3, convertedElements.length, "Array should have three elements");
    assertEquals("apple", convertedElements[0], "First element should be 'apple'");
    assertEquals("banana", convertedElements[1], "Second element should be 'banana'");
    assertEquals("cherry", convertedElements[2], "Third element should be 'cherry'");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"), times(1));
  }

  /** Test the constructor with nested STRUCT elements. */
  @Test
  public void constructor_ShouldConvertNestedStructElementsProperly() throws SQLException {
    String metadata = "ARRAY<STRUCT<id:INT,name:STRING>>";
    Map<String, Object> struct1 = new LinkedHashMap<>();
    struct1.put("id", 1);
    struct1.put("name", "Alice");

    Map<String, Object> struct2 = new LinkedHashMap<>();
    struct2.put("id", 2);
    struct2.put("name", "Bob");

    List<Object> originalList = Arrays.asList(struct1, struct2);

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRUCT<id:INT,name:STRING>>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRUCT<id:INT,name:STRING>>"))
        .thenReturn("STRUCT<id:INT,name:STRING>");

    // Mock MetadataParser.parseStructMetadata for each struct metadata string
    Map<String, Object> parsedStructType = new LinkedHashMap<>();
    parsedStructType.put("id", "INT");
    parsedStructType.put("name", "STRING");
    mockParseStructMetadata("STRUCT<id:INT,name:STRING>", parsedStructType);

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Retrieve the array
    Object arrayObj = databricksArray.getArray();
    assertNotNull(arrayObj, "getArray() should not return null");
    assertTrue(arrayObj instanceof Object[], "getArray() should return an Object[]");
    Object[] convertedElements = (Object[]) arrayObj;

    // Assertions
    assertEquals(2, convertedElements.length, "Array should have two elements");

    // Verify first STRUCT
    assertTrue(
        convertedElements[0] instanceof DatabricksStruct,
        "First element should be an instance of DatabricksStruct");
    DatabricksStruct convertedStruct1 = (DatabricksStruct) convertedElements[0];
    Object[] struct1Attributes = convertedStruct1.getAttributes();
    assertEquals(2, struct1Attributes.length, "First struct should have two attributes");
    assertEquals(1, struct1Attributes[0], "First struct 'id' should be 1");
    assertEquals("Alice", struct1Attributes[1], "First struct 'name' should be 'Alice'");

    // Verify second STRUCT
    assertTrue(
        convertedElements[1] instanceof DatabricksStruct,
        "Second element should be an instance of DatabricksStruct");
    DatabricksStruct convertedStruct2 = (DatabricksStruct) convertedElements[1];
    Object[] struct2Attributes = convertedStruct2.getAttributes();
    assertEquals(2, struct2Attributes.length, "Second struct should have two attributes");
    assertEquals(2, struct2Attributes[0], "Second struct 'id' should be 2");
    assertEquals("Bob", struct2Attributes[1], "Second struct 'name' should be 'Bob'");

    // Verify that the mocks were called as expected
    metadataParserMock.verify(
        () -> MetadataParser.parseArrayMetadata("ARRAY<STRUCT<id:INT,name:STRING>>"), times(1));
    metadataParserMock.verify(
        () -> MetadataParser.parseStructMetadata("STRUCT<id:INT,name:STRING>"),
        times(2)); // Called for each struct
  }

  /** Test the constructor with nested ARRAY elements. */
  @Test
  public void constructor_ShouldHandleNestedArraysCorrectly() throws SQLException {
    String metadata = "ARRAY<ARRAY<STRING>>";
    List<Object> innerArray1 = Arrays.asList("apple", "banana");
    List<Object> innerArray2 = Arrays.asList("cherry", "date");
    List<Object> originalList = Arrays.asList(innerArray1, innerArray2);

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<ARRAY<STRING>>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<ARRAY<STRING>>"))
        .thenReturn("ARRAY<STRING>");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Retrieve the array
    Object arrayObj = databricksArray.getArray();
    assertNotNull(arrayObj, "getArray() should not return null");
    assertTrue(arrayObj instanceof Object[], "getArray() should return an Object[]");
    Object[] outerElements = (Object[]) arrayObj;

    // Assertions
    assertEquals(2, outerElements.length, "Outer array should have two elements");

    // Verify first inner ARRAY
    assertTrue(
        outerElements[0] instanceof DatabricksArray,
        "First inner element should be an instance of DatabricksArray");
    DatabricksArray convertedInnerArray1 = (DatabricksArray) outerElements[0];
    Object[] innerElements1 = (Object[]) convertedInnerArray1.getArray();
    assertEquals(2, innerElements1.length, "First inner array should have two elements");
    assertEquals("apple", innerElements1[0], "First inner element should be 'apple'");
    assertEquals("banana", innerElements1[1], "Second inner element should be 'banana'");

    // Verify second inner ARRAY
    assertTrue(
        outerElements[1] instanceof DatabricksArray,
        "Second inner element should be an instance of DatabricksArray");
    DatabricksArray convertedInnerArray2 = (DatabricksArray) outerElements[1];
    Object[] innerElements2 = (Object[]) convertedInnerArray2.getArray();
    assertEquals(2, innerElements2.length, "Second inner array should have two elements");
    assertEquals("cherry", innerElements2[0], "First inner element should be 'cherry'");
    assertEquals("date", innerElements2[1], "Second inner element should be 'date'");

    // Verify that the mocks were called as expected
    metadataParserMock.verify(
        () -> MetadataParser.parseArrayMetadata("ARRAY<ARRAY<STRING>>"), times(1));
    metadataParserMock.verify(
        () -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"),
        times(2)); // Called for each inner array
  }

  /**
   * Test the constructor with invalid STRUCT elements (expecting STRUCT but provided non-STRUCT).
   * Expects IllegalArgumentException.
   */
  @Test
  public void constructor_ShouldThrowException_WhenStructElementIsNotStruct() throws SQLException {
    String metadata = "ARRAY<STRUCT<id:INT,name:STRING>>";
    // Providing a non-Map element for STRUCT
    List<Object> originalList = Arrays.asList("invalid_struct", new LinkedHashMap<>());

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRUCT<id:INT,name:STRING>>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRUCT<id:INT,name:STRING>>"))
        .thenReturn("STRUCT<id:INT,name:STRING>");

    // Mock MetadataParser.parseStructMetadata for each struct metadata string
    Map<String, Object> parsedStructType = new LinkedHashMap<>();
    parsedStructType.put("id", "INT");
    parsedStructType.put("name", "STRING");
    mockParseStructMetadata("STRUCT<id:INT,name:STRING>", parsedStructType);

    // Expecting IllegalArgumentException due to first element being a String instead of a Map
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              new DatabricksArray(originalList, metadata);
            },
            "Constructor should throw IllegalArgumentException for invalid STRUCT elements");

    assertTrue(
        exception.getMessage().contains("Error converting elements"),
        "Exception message should indicate expected STRUCT but found String");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(
        () -> MetadataParser.parseArrayMetadata("ARRAY<STRUCT<id:INT,name:STRING>>"), times(1));
  }

  /**
   * Test the constructor with invalid ARRAY elements (expecting ARRAY but provided non-ARRAY).
   * Expects IllegalArgumentException.
   */
  @Test
  public void constructor_ShouldThrowException_WhenArrayElementIsNotArray() throws SQLException {
    String metadata = "ARRAY<ARRAY<STRING>>";
    // Providing a non-List element for ARRAY
    List<Object> originalList = Arrays.asList("invalid_array", Arrays.asList("apple", "banana"));

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<ARRAY<STRING>>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<ARRAY<STRING>>"))
        .thenReturn("ARRAY<STRING>");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    // Expecting IllegalArgumentException due to first element being a String instead of a List
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              new DatabricksArray(originalList, metadata);
            },
            "Constructor should throw IllegalArgumentException for invalid ARRAY elements");

    assertTrue(
        exception.getMessage().contains("Error converting elements"),
        "Exception message should indicate expected ARRAY but found String");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(
        () -> MetadataParser.parseArrayMetadata("ARRAY<ARRAY<STRING>>"), times(1));
  }

  /** Test the constructor with null metadata. Expects NullPointerException. */
  @Test
  public void constructor_ShouldThrowNullPointerException_WhenMetadataIsNull() throws SQLException {
    String metadata = null;
    List<Object> originalList = Arrays.asList("apple", "banana");

    // Mock MetadataParser.parseArrayMetadata to throw exception when metadata is null
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata(null))
        .thenThrow(new NullPointerException("Metadata cannot be null"));

    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> {
              new DatabricksArray(originalList, metadata);
            },
            "Constructor should throw NullPointerException when metadata is null");

    assertEquals(
        "Metadata cannot be null",
        exception.getMessage(),
        "Exception message should indicate that metadata cannot be null");

    // Verify that the mock was called once with null metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata(null), times(1));
  }

  /** Test the constructor with an empty array. */
  @Test
  public void constructor_ShouldHandleEmptyArrayProperly() throws SQLException {
    String metadata = "ARRAY<STRING>";
    List<Object> originalList = Collections.emptyList();

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Retrieve the array
    Object arrayObj = databricksArray.getArray();
    assertNotNull(arrayObj, "getArray() should not return null");
    assertTrue(arrayObj instanceof Object[], "getArray() should return an Object[]");
    Object[] convertedElements = (Object[]) arrayObj;

    // Assertions
    assertEquals(0, convertedElements.length, "Array should be empty");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"), times(1));
  }

  /** Test the constructor with null elements within the array. */
  @Test
  public void constructor_ShouldHandleNullElementsWithinArray() throws SQLException {
    String metadata = "ARRAY<STRING>";
    List<Object> originalList = Arrays.asList("apple", null, "cherry");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Retrieve the array
    Object arrayObj = databricksArray.getArray();
    assertNotNull(arrayObj, "getArray() should not return null");
    assertTrue(arrayObj instanceof Object[], "getArray() should return an Object[]");
    Object[] convertedElements = (Object[]) arrayObj;

    // Assertions
    assertEquals(3, convertedElements.length, "Array should have three elements");
    assertEquals("apple", convertedElements[0], "First element should be 'apple'");
    assertNull(convertedElements[1], "Second element should be null");
    assertEquals("cherry", convertedElements[2], "Third element should be 'cherry'");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"), times(1));
  }

  /** Test the constructor with elements requiring type conversion from String to Integer. */
  @Test
  public void constructor_ShouldConvertStringToIntegerSuccessfully() throws SQLException {
    String metadata = "ARRAY<INT>";
    List<Object> originalList = Arrays.asList("1", "2", "3"); // Strings to be converted to Integers

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<INT>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<INT>"))
        .thenReturn("INT");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Retrieve the array
    Object arrayObj = databricksArray.getArray();
    assertNotNull(arrayObj, "getArray() should not return null");
    assertTrue(arrayObj instanceof Object[], "getArray() should return an Object[]");
    Object[] convertedElements = (Object[]) arrayObj;

    // Assertions
    assertEquals(3, convertedElements.length, "Array should have three elements");
    assertEquals(1, convertedElements[0], "First element should be 1");
    assertEquals(2, convertedElements[1], "Second element should be 2");
    assertEquals(3, convertedElements[2], "Third element should be 3");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<INT>"), times(1));
  }

  /** Test the constructor with binary type elements. */
  @Test
  public void constructor_ShouldHandleBinaryElementsCorrectly() throws SQLException {
    String metadata = "ARRAY<BINARY>";
    List<Object> originalList = Arrays.asList("binary1".getBytes(), "binary2".getBytes());

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<BINARY>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<BINARY>"))
        .thenReturn("BINARY");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Retrieve the array
    Object arrayObj = databricksArray.getArray();
    assertNotNull(arrayObj, "getArray() should not return null");
    assertTrue(arrayObj instanceof Object[], "getArray() should return an Object[]");
    Object[] convertedElements = (Object[]) arrayObj;

    // Assertions
    assertEquals(2, convertedElements.length, "Array should have two elements");
    assertArrayEquals(
        "binary1".getBytes(), (byte[]) convertedElements[0], "First binary element should match");
    assertArrayEquals(
        "binary2".getBytes(), (byte[]) convertedElements[1], "Second binary element should match");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<BINARY>"), times(1));
  }

  /** Test the constructor with DATE type elements. */
  @Test
  public void constructor_ShouldConvertStringToDateSuccessfully() throws SQLException {
    String metadata = "ARRAY<DATE>";
    List<Object> originalList = Arrays.asList("2024-01-01", "2024-12-31");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<DATE>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<DATE>"))
        .thenReturn("DATE");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Retrieve the array
    Object arrayObj = databricksArray.getArray();
    assertNotNull(arrayObj, "getArray() should not return null");
    assertTrue(arrayObj instanceof Object[], "getArray() should return an Object[]");
    Object[] convertedElements = (Object[]) arrayObj;

    // Assertions
    assertEquals(2, convertedElements.length, "Array should have two elements");
    assertEquals(
        Date.valueOf("2024-01-01"),
        convertedElements[0],
        "First date element should be '2024-01-01'");
    assertEquals(
        Date.valueOf("2024-12-31"),
        convertedElements[1],
        "Second date element should be '2024-12-31'");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<DATE>"), times(1));
  }

  /** Test the constructor with TIMESTAMP type elements. */
  @Test
  public void constructor_ShouldConvertStringToTimestampSuccessfully() throws SQLException {
    String metadata = "ARRAY<TIMESTAMP>";
    List<Object> originalList = Arrays.asList("2024-01-01 10:00:00", "2024-12-31 23:59:59");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<TIMESTAMP>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<TIMESTAMP>"))
        .thenReturn("TIMESTAMP");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Retrieve the array
    Object arrayObj = databricksArray.getArray();
    assertNotNull(arrayObj, "getArray() should not return null");
    assertTrue(arrayObj instanceof Object[], "getArray() should return an Object[]");
    Object[] convertedElements = (Object[]) arrayObj;

    // Assertions
    assertEquals(2, convertedElements.length, "Array should have two elements");
    assertEquals(
        Timestamp.valueOf("2024-01-01 10:00:00"),
        convertedElements[0],
        "First timestamp element should be '2024-01-01 10:00:00'");
    assertEquals(
        Timestamp.valueOf("2024-12-31 23:59:59"),
        convertedElements[1],
        "Second timestamp element should be '2024-12-31 23:59:59'");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(
        () -> MetadataParser.parseArrayMetadata("ARRAY<TIMESTAMP>"), times(1));
  }

  /** Test the constructor with BOOLEAN type elements. */
  @Test
  public void constructor_ShouldConvertStringToBooleanSuccessfully() throws SQLException {
    String metadata = "ARRAY<BOOLEAN>";
    List<Object> originalList = Arrays.asList("true", "false", "TRUE", "FALSE");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<BOOLEAN>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<BOOLEAN>"))
        .thenReturn("BOOLEAN");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Retrieve the array
    Object arrayObj = databricksArray.getArray();
    assertNotNull(arrayObj, "getArray() should not return null");
    assertTrue(arrayObj instanceof Object[], "getArray() should return an Object[]");
    Object[] convertedElements = (Object[]) arrayObj;

    // Assertions
    assertEquals(4, convertedElements.length, "Array should have four elements");
    assertEquals(true, convertedElements[0], "First boolean element should be true");
    assertEquals(false, convertedElements[1], "Second boolean element should be false");
    assertEquals(true, convertedElements[2], "Third boolean element should be true");
    assertEquals(false, convertedElements[3], "Fourth boolean element should be false");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<BOOLEAN>"), times(1));
  }

  /** Test the constructor with DECIMAL type elements. */
  @Test
  public void constructor_ShouldConvertStringToDecimalSuccessfully() throws SQLException {
    String metadata = "ARRAY<DECIMAL>";
    List<Object> originalList = Arrays.asList("10.5", "20.75", "30.00");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<DECIMAL>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<DECIMAL>"))
        .thenReturn("DECIMAL");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Retrieve the array
    Object arrayObj = databricksArray.getArray();
    assertNotNull(arrayObj, "getArray() should not return null");
    assertTrue(arrayObj instanceof Object[], "getArray() should return an Object[]");
    Object[] convertedElements = (Object[]) arrayObj;

    // Assertions
    assertEquals(3, convertedElements.length, "Array should have three elements");
    assertEquals(
        new BigDecimal("10.5"), convertedElements[0], "First decimal element should be 10.5");
    assertEquals(
        new BigDecimal("20.75"), convertedElements[1], "Second decimal element should be 20.75");
    assertEquals(
        new BigDecimal("30.00"), convertedElements[2], "Third decimal element should be 30.00");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<DECIMAL>"), times(1));
  }

  /**
   * Test the free() method. Since free() does not have observable effects, ensure it does not throw
   * exceptions.
   */
  @Test
  public void free_ShouldNotThrowException() throws SQLException {
    String metadata = "ARRAY<STRING>";
    List<Object> originalList = Arrays.asList("apple", "banana");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Ensure that calling free() does not throw an exception
    assertDoesNotThrow(
        () -> {
          databricksArray.free();
        },
        "free() method should not throw any exceptions");
  }

  /**
   * Test the getResultSet() methods. These methods are not implemented and should throw
   * UnsupportedOperationException.
   */
  @Test
  public void getResultSet_ShouldThrowUnsupportedOperationException() throws SQLException {
    String metadata = "ARRAY<STRING>";
    List<Object> originalList = Arrays.asList("apple", "banana");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Test getResultSet()
    UnsupportedOperationException exception1 =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              databricksArray.getResultSet();
            },
            "getResultSet() should throw UnsupportedOperationException");
    assertEquals(
        "getResultSet() not implemented",
        exception1.getMessage(),
        "Exception message should match the implementation");

    // Test getResultSet(Map<String, Class<?>> map)
    UnsupportedOperationException exception2 =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              databricksArray.getResultSet(new LinkedHashMap<>());
            },
            "getResultSet(Map) should throw UnsupportedOperationException");
    assertEquals(
        "getResultSet(Map<String, Class<?>> map) not implemented",
        exception2.getMessage(),
        "Exception message should match the implementation");

    // Test getResultSet(long index, int count)
    UnsupportedOperationException exception3 =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              databricksArray.getResultSet(1, 2);
            },
            "getResultSet(long, int) should throw UnsupportedOperationException");
    assertEquals(
        "getResultSet(long index, int count) not implemented",
        exception3.getMessage(),
        "Exception message should match the implementation");

    // Test getResultSet(long index, int count, Map<String, Class<?>> map)
    UnsupportedOperationException exception4 =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              databricksArray.getResultSet(1, 2, new LinkedHashMap<>());
            },
            "getResultSet(long, int, Map) should throw UnsupportedOperationException");
    assertEquals(
        "getResultSet(long index, int count, Map<String, Class<?>> map) not implemented",
        exception4.getMessage(),
        "Exception message should match the implementation");
  }

  /** Test the constructor with mixed null and non-null elements. */
  @Test
  public void constructor_ShouldHandleMixedNullAndNonNullElementsProperly() throws SQLException {
    String metadata = "ARRAY<STRING>";
    List<Object> originalList = Arrays.asList("apple", null, "cherry");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Retrieve the array
    Object arrayObj = databricksArray.getArray();
    assertNotNull(arrayObj, "getArray() should not return null");
    assertTrue(arrayObj instanceof Object[], "getArray() should return an Object[]");
    Object[] convertedElements = (Object[]) arrayObj;

    // Assertions
    assertEquals(3, convertedElements.length, "Array should have three elements");
    assertEquals("apple", convertedElements[0], "First element should be 'apple'");
    assertNull(convertedElements[1], "Second element should be null");
    assertEquals("cherry", convertedElements[2], "Third element should be 'cherry'");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"), times(1));
  }

  /** Test the getArray(long index, int count) method with valid subset. */
  @Test
  public void getArray_ShouldReturnValidSubset_WhenIndexAndCountAreWithinBounds()
      throws SQLException {
    String metadata = "ARRAY<STRING>";
    List<Object> originalList = Arrays.asList("apple", "banana", "cherry", "date", "elderberry");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Retrieve a subset of the array (1-based index)
    Object subsetObj = databricksArray.getArray(2, 3); // Should retrieve "banana", "cherry", "date"
    assertNotNull(subsetObj, "getArray(index, count) should not return null");
    assertTrue(subsetObj instanceof Object[], "getArray(index, count) should return an Object[]");
    Object[] subset = (Object[]) subsetObj;

    // Assertions
    assertEquals(3, subset.length, "Subset array should have three elements");
    assertEquals("banana", subset[0], "First element of subset should be 'banana'");
    assertEquals("cherry", subset[1], "Second element of subset should be 'cherry'");
    assertEquals("date", subset[2], "Third element of subset should be 'date'");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"), times(1));
  }

  /**
   * Test the getArray(long index, int count) method with out-of-bounds index. Expects
   * ArrayIndexOutOfBoundsException.
   */
  @Test
  public void getArray_ShouldThrowException_WhenIndexIsOutOfBounds() throws SQLException {
    String metadata = "ARRAY<STRING>";
    List<Object> originalList = Arrays.asList("apple", "banana", "cherry");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Attempt to retrieve a subset starting beyond the array length
    ArrayIndexOutOfBoundsException exception =
        assertThrows(
            ArrayIndexOutOfBoundsException.class,
            () -> {
              databricksArray.getArray(5, 2); // Index 5 is out of bounds for a 3-element array
            },
            "getArray(index, count) should throw ArrayIndexOutOfBoundsException for out-of-bounds index");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"), times(1));
  }

  /** Test the getBaseTypeName() method. */
  @Test
  public void getBaseTypeName_ShouldReturnCorrectMetadata() throws SQLException {
    String metadata = "ARRAY<STRING>";
    List<Object> originalList = Arrays.asList("apple", "banana");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    String baseTypeName = databricksArray.getBaseTypeName();
    assertEquals(
        "ARRAY<STRING>", baseTypeName, "getBaseTypeName() should return the original metadata");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"), times(1));
  }

  /** Test the getBaseType() method. */
  @Test
  public void getBaseType_ShouldReturnSqlTypesOther() throws SQLException {
    String metadata = "ARRAY<STRING>";
    List<Object> originalList = Arrays.asList("apple", "banana");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    int baseType = databricksArray.getBaseType();
    assertEquals(
        java.sql.Types.OTHER, baseType, "getBaseType() should return java.sql.Types.OTHER");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"), times(1));
  }

  @Test
  public void testToString_WithStringElements_ShouldProduceJsonLikeString() throws SQLException {
    // Arrange
    String metadata = "ARRAY<STRING>";
    // Mock the parser so DatabricksArray knows to treat elements as STRING
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    // Suppose the array is ["apple", "banana"]
    List<Object> originalList = Arrays.asList("apple", "banana");

    // Create the DatabricksArray
    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Act: call toString()
    String actual = databricksArray.toString();

    // Assert:
    // Expect each string element in double-quotes, overall in bracket notation: ["apple","banana"]
    String expected = "[\"apple\",\"banana\"]";
    assertEquals(
        expected,
        actual,
        "DatabricksArray.toString() should produce a JSON-like array with quoted string elements");

    // Verify that parseArrayMetadata was called once for this metadata
    metadataParserMock.verify(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"), times(1));
  }
}
