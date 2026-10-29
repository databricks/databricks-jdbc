package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
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
   * Expects DatabricksDriverException.
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

    // Expecting DatabricksDriverException due to first element being a String instead of a Map
    DatabricksDriverException exception =
        assertThrows(
            DatabricksDriverException.class,
            () -> {
              new DatabricksArray(originalList, metadata);
            },
            "Constructor should throw DatabricksDriverException for invalid STRUCT elements");

    assertTrue(
        exception.getMessage().contains("Error converting elements"),
        "Exception message should indicate expected STRUCT but found String");

    // Verify that the mock was called once with the correct metadata
    metadataParserMock.verify(
        () -> MetadataParser.parseArrayMetadata("ARRAY<STRUCT<id:INT,name:STRING>>"), times(1));
  }

  /**
   * Test the constructor with invalid ARRAY elements (expecting ARRAY but provided non-ARRAY).
   * Expects DatabricksDriverException.
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

    // Expecting DatabricksDriverException due to first element being a String instead of a List
    DatabricksDriverException exception =
        assertThrows(
            DatabricksDriverException.class,
            () -> {
              new DatabricksArray(originalList, metadata);
            },
            "Constructor should throw DatabricksDriverException for invalid ARRAY elements");

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
   * Test the getResultSet() methods. Per the JDBC {@link java.sql.Array#getResultSet()} contract,
   * these return a ResultSet with one row per element, where column 1 is the 1-based element index
   * and column 2 is the element value.
   */
  @Test
  public void getResultSet_ShouldReturnResultSetOfElements() throws SQLException {
    String metadata = "ARRAY<STRING>";
    List<Object> originalList = Arrays.asList("apple", "banana", "cherry");

    // Mock MetadataParser.parseArrayMetadata for "ARRAY<STRING>"
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");

    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);

    // Test getResultSet()
    try (java.sql.ResultSet rs = databricksArray.getResultSet()) {
      assertNotNull(rs);
      String[] expected = {"apple", "banana", "cherry"};
      int row = 0;
      while (rs.next()) {
        row++;
        assertEquals(row, rs.getLong(1));
        assertEquals(expected[row - 1], rs.getString(2));
      }
      assertEquals(3, row);
    }

    // Test getResultSet(Map<String, Class<?>> map)
    try (java.sql.ResultSet rs = databricksArray.getResultSet(new LinkedHashMap<>())) {
      assertNotNull(rs);
      int row = 0;
      while (rs.next()) {
        row++;
      }
      assertEquals(3, row);
    }

    // Test getResultSet(long index, int count) windowed variant
    try (java.sql.ResultSet rs = databricksArray.getResultSet(2, 2)) {
      assertNotNull(rs);
      long[] expectedIndex = {2, 3};
      String[] expectedValue = {"banana", "cherry"};
      int row = 0;
      while (rs.next()) {
        assertEquals(expectedIndex[row], rs.getLong(1));
        assertEquals(expectedValue[row], rs.getString(2));
        row++;
      }
      assertEquals(2, row);
    }

    // Test getResultSet(long index, int count, Map<String, Class<?>> map)
    try (java.sql.ResultSet rs = databricksArray.getResultSet(1, 1, new LinkedHashMap<>())) {
      assertNotNull(rs);
      assertTrue(rs.next());
      assertEquals(1, rs.getLong(1));
      assertEquals("apple", rs.getString(2));
      assertFalse(rs.next());
    }
  }

  /** getResultSet(index, Integer.MAX_VALUE) must return all remaining elements (no overflow). */
  @Test
  public void getResultSet_WithMaxCount_ShouldReturnRemainingElements() throws SQLException {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");
    DatabricksArray databricksArray =
        new DatabricksArray(Arrays.asList("apple", "banana", "cherry"), "ARRAY<STRING>");

    try (java.sql.ResultSet rs = databricksArray.getResultSet(2, Integer.MAX_VALUE)) {
      long[] expectedIndex = {2, 3};
      String[] expectedValue = {"banana", "cherry"};
      int row = 0;
      while (rs.next()) {
        assertEquals(expectedIndex[row], rs.getLong(1));
        assertEquals(expectedValue[row], rs.getString(2));
        row++;
      }
      assertEquals(2, row);
    }
  }

  /** An index past the end of the array (including > Integer.MAX_VALUE) yields an empty set. */
  @Test
  public void getResultSet_WithIndexPastEnd_ShouldReturnEmptyResultSet() throws SQLException {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");
    DatabricksArray databricksArray =
        new DatabricksArray(Arrays.asList("apple", "banana", "cherry"), "ARRAY<STRING>");

    try (java.sql.ResultSet rs = databricksArray.getResultSet(5, 2)) {
      assertFalse(rs.next());
    }
    try (java.sql.ResultSet rs = databricksArray.getResultSet(4294967296L, 2)) {
      assertFalse(rs.next());
    }
    try (java.sql.ResultSet rs = databricksArray.getResultSet(4294967296L, Integer.MAX_VALUE)) {
      assertFalse(rs.next());
    }
  }

  /** An index < 1 must raise a checked SQLException carrying the input-validation error code. */
  @Test
  public void getResultSet_WithIndexBelowOne_ShouldThrowSQLException() {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");
    DatabricksArray databricksArray =
        new DatabricksArray(Arrays.asList("apple", "banana", "cherry"), "ARRAY<STRING>");

    for (long badIndex : new long[] {0L, -1L}) {
      SQLException exception =
          assertThrows(SQLException.class, () -> databricksArray.getResultSet(badIndex, 2));
      assertInstanceOf(DatabricksSQLException.class, exception);
      assertEquals("INPUT_VALIDATION_ERROR", exception.getSQLState());
      assertEquals(1015, exception.getErrorCode());
      assertTrue(exception.getMessage().contains("Array index must be >= 1"));
    }
    assertThrows(
        DatabricksSQLException.class,
        () -> databricksArray.getResultSet(0, 1, new LinkedHashMap<>()));
  }

  /** A negative count must raise a checked SQLException carrying the input-validation code. */
  @Test
  public void getResultSet_WithNegativeCount_ShouldThrowSQLException() {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");
    DatabricksArray databricksArray =
        new DatabricksArray(Arrays.asList("apple", "banana", "cherry"), "ARRAY<STRING>");

    for (int badCount : new int[] {-1, Integer.MIN_VALUE}) {
      SQLException exception =
          assertThrows(SQLException.class, () -> databricksArray.getResultSet(1, badCount));
      assertInstanceOf(DatabricksSQLException.class, exception);
      assertEquals("INPUT_VALIDATION_ERROR", exception.getSQLState());
      assertEquals(1015, exception.getErrorCode());
      assertTrue(exception.getMessage().contains("Array element count must be >= 0"));
    }
    assertThrows(
        DatabricksSQLException.class,
        () -> databricksArray.getResultSet(1, -1, new LinkedHashMap<>()));
  }

  /** A non-empty type map must be rejected rather than silently ignored; null is accepted. */
  @Test
  public void getResultSet_WithNonEmptyTypeMap_ShouldThrowFeatureNotSupported()
      throws SQLException {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");
    DatabricksArray databricksArray =
        new DatabricksArray(Arrays.asList("apple", "banana", "cherry"), "ARRAY<STRING>");
    Map<String, Class<?>> typeMap = new LinkedHashMap<>();
    typeMap.put("STRING", String.class);

    SQLException exception =
        assertThrows(
            DatabricksSQLFeatureNotSupportedException.class,
            () -> databricksArray.getResultSet(typeMap));
    assertEquals("UNSUPPORTED_OPERATION", exception.getSQLState());
    assertEquals(1005, exception.getErrorCode());
    SQLException windowedException =
        assertThrows(
            DatabricksSQLFeatureNotSupportedException.class,
            () -> databricksArray.getResultSet(1, 1, typeMap));
    assertEquals("UNSUPPORTED_OPERATION", windowedException.getSQLState());
    assertEquals(1005, windowedException.getErrorCode());

    try (java.sql.ResultSet rs = databricksArray.getResultSet(null)) {
      int row = 0;
      while (rs.next()) {
        row++;
      }
      assertEquals(3, row);
    }
    try (java.sql.ResultSet rs = databricksArray.getResultSet(1, 1, null)) {
      assertTrue(rs.next());
      assertEquals("apple", rs.getString(2));
      assertFalse(rs.next());
    }
  }

  /** The VALUE column type must be derived from the base element type, ignoring parameters. */
  @Test
  public void getResultSet_MetadataShouldReflectParameterizedAndNestedElementTypes()
      throws SQLException {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<DECIMAL(10,2)>"))
        .thenReturn("DECIMAL(10,2)");
    DatabricksArray decimalArray =
        new DatabricksArray(Arrays.asList("1.50", "2.25"), "ARRAY<DECIMAL(10,2)>");
    try (java.sql.ResultSet rs = decimalArray.getResultSet()) {
      assertEquals(java.sql.Types.BIGINT, rs.getMetaData().getColumnType(1));
      assertEquals(java.sql.Types.DECIMAL, rs.getMetaData().getColumnType(2));
      assertEquals(19, rs.getMetaData().getPrecision(1));
      assertEquals(0, rs.getMetaData().getScale(1));
      assertEquals(10, rs.getMetaData().getPrecision(2));
      assertEquals(2, rs.getMetaData().getScale(2));
      assertEquals("java.math.BigDecimal", rs.getMetaData().getColumnClassName(2));
      assertTrue(rs.next());
      assertEquals(new java.math.BigDecimal("1.50"), rs.getObject(2));
      assertEquals(new java.math.BigDecimal("1.50"), rs.getBigDecimal(2));
      assertTrue(rs.next());
      assertEquals(new java.math.BigDecimal("2.25"), rs.getObject(2));
      assertFalse(rs.next());
    }

    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<ARRAY<INT>>"))
        .thenReturn("ARRAY<INT>");
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<INT>"))
        .thenReturn("INT");
    DatabricksArray nestedArray =
        new DatabricksArray(
            Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3)), "ARRAY<ARRAY<INT>>");
    try (java.sql.ResultSet rs = nestedArray.getResultSet()) {
      assertEquals(java.sql.Types.ARRAY, rs.getMetaData().getColumnType(2));
    }

    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");
    DatabricksArray stringArray = new DatabricksArray(Arrays.asList("a"), "ARRAY<STRING>");
    try (java.sql.ResultSet rs = stringArray.getResultSet()) {
      assertEquals(java.sql.Types.VARCHAR, rs.getMetaData().getColumnType(2));
    }
  }

  @Test
  public void getResultSet_ShouldPreserveExistingArrayValuesAndStringRepresentation()
      throws SQLException {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<DECIMAL(10,2)>"))
        .thenReturn("DECIMAL(10,2)");
    DatabricksArray decimalArray =
        new DatabricksArray(Arrays.asList("1.50", "2.25"), "ARRAY<DECIMAL(10,2)>");

    Object[] expected = {"1.50", "2.25"};
    assertArrayEquals(expected, (Object[]) decimalArray.getArray());
    assertEquals("[\"1.50\",\"2.25\"]", decimalArray.toString());
    try (java.sql.ResultSet rs = decimalArray.getResultSet()) {
      assertTrue(rs.next());
      assertEquals(new BigDecimal("1.50"), rs.getObject(2));
      assertTrue(rs.next());
      assertEquals(new BigDecimal("2.25"), rs.getObject(2));
      assertFalse(rs.next());
    }
    assertArrayEquals(expected, (Object[]) decimalArray.getArray());
    assertEquals("[\"1.50\",\"2.25\"]", decimalArray.toString());
  }

  /** A collated string element type must report VARCHAR for the VALUE column, not OTHER. */
  @Test
  public void getResultSet_MetadataShouldReportVarcharForCollatedStringElementType()
      throws SQLException {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING COLLATE UTF8_LCASE>"))
        .thenReturn("STRING COLLATE UTF8_LCASE");
    DatabricksArray collatedArray =
        new DatabricksArray(Arrays.asList("a", "B"), "ARRAY<STRING COLLATE UTF8_LCASE>");
    try (java.sql.ResultSet rs = collatedArray.getResultSet()) {
      assertEquals(java.sql.Types.BIGINT, rs.getMetaData().getColumnType(1));
      assertEquals(java.sql.Types.VARCHAR, rs.getMetaData().getColumnType(2));
    }
  }

  /** TIMESTAMP and TIMESTAMP_NTZ element types must not break result-set metadata construction. */
  @Test
  public void getResultSet_ShouldSupportTimestampElementTypes() throws SQLException {
    Timestamp ts1 = Timestamp.valueOf("2024-01-01 12:30:45.123");
    Timestamp ts2 = Timestamp.valueOf("2024-02-15 08:15:30.456");
    for (String elementType : new String[] {"TIMESTAMP", "TIMESTAMP_NTZ"}) {
      String arrayType = "ARRAY<" + elementType + ">";
      metadataParserMock
          .when(() -> MetadataParser.parseArrayMetadata(arrayType))
          .thenReturn(elementType);
      DatabricksArray timestampArray = new DatabricksArray(List.of(ts1, ts2), arrayType);
      try (java.sql.ResultSet rs = timestampArray.getResultSet()) {
        assertEquals(java.sql.Types.BIGINT, rs.getMetaData().getColumnType(1));
        assertEquals(java.sql.Types.TIMESTAMP, rs.getMetaData().getColumnType(2), elementType);
        assertEquals("java.sql.Timestamp", rs.getMetaData().getColumnClassName(2), elementType);
        assertTrue(rs.next());
        assertEquals(1L, rs.getLong(1));
        assertEquals(ts1, rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(2L, rs.getLong(1));
        assertEquals(ts2, rs.getObject(2));
        assertFalse(rs.next());
      }
    }
  }

  /** Types.OTHER element types must report their real column class name, not "null". */
  @Test
  public void getResultSet_ShouldReportClassNameForOtherElementTypes() throws SQLException {
    assertOtherElementClassName("VARIANT", ColumnInfoTypeName.VARIANT);
    assertOtherElementClassName("GEOMETRY(4326)", ColumnInfoTypeName.GEOMETRY);
    assertOtherElementClassName("GEOGRAPHY", ColumnInfoTypeName.GEOGRAPHY);
  }

  private void assertOtherElementClassName(String elementType, ColumnInfoTypeName expectedType)
      throws SQLException {
    String arrayType = "ARRAY<" + elementType + ">";
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata(arrayType))
        .thenReturn(elementType);
    DatabricksArray array = new DatabricksArray(Collections.emptyList(), arrayType);
    try (java.sql.ResultSet rs = array.getResultSet()) {
      assertEquals(java.sql.Types.OTHER, rs.getMetaData().getColumnType(2), elementType);
      String className = rs.getMetaData().getColumnClassName(2);
      assertNotEquals("null", className, elementType);
      assertEquals(DatabricksTypeUtil.getColumnTypeClassName(expectedType), className, elementType);
    }
  }

  /**
   * With geospatial support disabled, geospatial elements are held as string fallbacks; the VALUE
   * column must then report STRING so that its class name matches getObject().
   */
  @Test
  public void getResultSet_StringFallbackGeospatialElementsShouldReportString()
      throws SQLException {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<GEOMETRY(4326)>"))
        .thenReturn("GEOMETRY(4326)");
    DatabricksArray array =
        new DatabricksArray(Arrays.asList("SRID=4326;POINT(1 2)", null), "ARRAY<GEOMETRY(4326)>");
    try (java.sql.ResultSet rs = array.getResultSet()) {
      assertEquals(java.sql.Types.VARCHAR, rs.getMetaData().getColumnType(2));
      assertEquals("STRING", rs.getMetaData().getColumnTypeName(2));
      assertEquals("java.lang.String", rs.getMetaData().getColumnClassName(2));
      assertTrue(rs.next());
      Object value = rs.getObject(2);
      assertEquals("SRID=4326;POINT(1 2)", value);
      assertEquals(rs.getMetaData().getColumnClassName(2), value.getClass().getName());
      assertTrue(rs.next());
      assertNull(rs.getObject(2));
      assertFalse(rs.next());
    }
  }

  /**
   * An empty or all-null geospatial array has no element revealing whether geospatial support was
   * enabled, so the VALUE column deliberately keeps the declared geospatial type (Types.OTHER).
   */
  @Test
  public void getResultSet_EmptyOrAllNullGeospatialArrayShouldReportDeclaredType()
      throws SQLException {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<GEOGRAPHY>"))
        .thenReturn("GEOGRAPHY");
    String expectedClassName =
        DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.GEOGRAPHY);

    DatabricksArray emptyArray = new DatabricksArray(Collections.emptyList(), "ARRAY<GEOGRAPHY>");
    try (java.sql.ResultSet rs = emptyArray.getResultSet()) {
      assertEquals(java.sql.Types.OTHER, rs.getMetaData().getColumnType(2));
      assertEquals(expectedClassName, rs.getMetaData().getColumnClassName(2));
      assertFalse(rs.next());
    }

    DatabricksArray allNullArray =
        new DatabricksArray(Arrays.asList(null, null), "ARRAY<GEOGRAPHY>");
    try (java.sql.ResultSet rs = allNullArray.getResultSet()) {
      assertEquals(java.sql.Types.OTHER, rs.getMetaData().getColumnType(2));
      assertEquals(expectedClassName, rs.getMetaData().getColumnClassName(2));
      assertTrue(rs.next());
      assertNull(rs.getObject(2));
      assertTrue(rs.wasNull());
      assertTrue(rs.next());
      assertNull(rs.getObject(2));
      assertFalse(rs.next());
    }
  }

  /** Native geospatial elements must be reported, and returned, as the geospatial interface. */
  @Test
  public void getResultSet_NativeGeospatialElementsShouldReportGeospatialClass()
      throws SQLException {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<GEOMETRY(4326)>"))
        .thenReturn("GEOMETRY(4326)");
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(1 2)", 4326);
    DatabricksArray array =
        new DatabricksArray(Collections.singletonList(geometry), "ARRAY<GEOMETRY(4326)>");
    try (java.sql.ResultSet rs = array.getResultSet()) {
      assertEquals(java.sql.Types.OTHER, rs.getMetaData().getColumnType(2));
      String className = rs.getMetaData().getColumnClassName(2);
      assertEquals(DatabricksTypeUtil.GEOMETRY_CLASS_NAME, className);
      assertTrue(rs.next());
      Object value = rs.getObject(2);
      assertSame(geometry, value);
      assertTrue(com.databricks.jdbc.api.IGeometry.class.isInstance(value), className);
      assertFalse(rs.next());
    }
  }

  /** Nested complex elements must be readable via getArray/getObject on the element result set. */
  @Test
  public void getResultSet_NestedArrayElementsShouldBeReturnedAsDatabricksArray()
      throws SQLException {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<ARRAY<INT>>"))
        .thenReturn("ARRAY<INT>");
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<INT>"))
        .thenReturn("INT");
    DatabricksArray nestedArray =
        new DatabricksArray(
            Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3)), "ARRAY<ARRAY<INT>>");
    try (java.sql.ResultSet rs = nestedArray.getResultSet()) {
      assertTrue(rs.next());
      java.sql.Array first = rs.getArray(2);
      assertInstanceOf(DatabricksArray.class, first);
      assertArrayEquals(new Object[] {1, 2}, (Object[]) first.getArray());
      assertInstanceOf(DatabricksArray.class, rs.getObject(2));

      assertTrue(rs.next());
      java.sql.Array second = rs.getArray(2);
      assertInstanceOf(DatabricksArray.class, second);
      assertArrayEquals(new Object[] {3}, (Object[]) second.getArray());
      assertFalse(rs.next());
    }
  }

  /** An empty array must yield an empty result set with fully built metadata. */
  @Test
  public void getResultSet_EmptyArrayShouldReturnEmptyResultSetWithMetadata() throws SQLException {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");
    DatabricksArray emptyArray = new DatabricksArray(Collections.emptyList(), "ARRAY<STRING>");
    try (java.sql.ResultSet rs = emptyArray.getResultSet()) {
      assertEquals(2, rs.getMetaData().getColumnCount());
      assertEquals(java.sql.Types.BIGINT, rs.getMetaData().getColumnType(1));
      assertEquals(java.sql.Types.VARCHAR, rs.getMetaData().getColumnType(2));
      assertFalse(rs.next());
    }
  }

  /** STRUCT elements must be returned as DatabricksStruct and reported as Types.STRUCT. */
  @Test
  public void getResultSet_StructElementsShouldBeReturnedAsDatabricksStruct() throws SQLException {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRUCT<id:INT,name:STRING>>"))
        .thenReturn("STRUCT<id:INT,name:STRING>");
    Map<String, String> structType = new LinkedHashMap<>();
    structType.put("id", "INT");
    structType.put("name", "STRING");
    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata("STRUCT<id:INT,name:STRING>"))
        .thenReturn(structType);
    Map<String, Object> struct = new LinkedHashMap<>();
    struct.put("id", 1);
    struct.put("name", "alice");
    DatabricksArray structArray =
        new DatabricksArray(Arrays.asList(struct, null), "ARRAY<STRUCT<id:INT,name:STRING>>");
    try (java.sql.ResultSet rs = structArray.getResultSet()) {
      assertEquals(java.sql.Types.STRUCT, rs.getMetaData().getColumnType(2));
      assertTrue(rs.next());
      Object first = rs.getObject(2);
      assertInstanceOf(DatabricksStruct.class, first);
      assertArrayEquals(new Object[] {1, "alice"}, ((DatabricksStruct) first).getAttributes());
      assertFalse(rs.wasNull());
      assertTrue(rs.next());
      assertNull(rs.getObject(2));
      assertTrue(rs.wasNull());
      assertFalse(rs.next());
    }
  }

  /**
   * MAP elements must be returned as DatabricksMap. The VALUE column reports VARCHAR for MAP, which
   * matches how MAP columns are reported in query result metadata, while getColumnClassName reports
   * java.util.Map so it agrees with the value returned by getObject.
   */
  @Test
  public void getResultSet_MapElementsShouldBeReturnedAsDatabricksMap() throws SQLException {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<MAP<STRING,INT>>"))
        .thenReturn("MAP<STRING,INT>");
    metadataParserMock
        .when(() -> MetadataParser.parseMapMetadataParts("MAP<STRING,INT>"))
        .thenReturn(new String[] {"STRING", "INT"});
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("k", 10);
    DatabricksArray mapArray =
        new DatabricksArray(Collections.singletonList(map), "ARRAY<MAP<STRING,INT>>");
    try (java.sql.ResultSet rs = mapArray.getResultSet()) {
      assertEquals(java.sql.Types.VARCHAR, rs.getMetaData().getColumnType(2));
      assertEquals("java.util.Map", rs.getMetaData().getColumnClassName(2));
      assertTrue(rs.next());
      Object value = rs.getObject(2);
      assertInstanceOf(DatabricksMap.class, value);
      assertEquals(10, ((Map<?, ?>) value).get("k"));
      assertFalse(rs.next());
    }
  }

  /** A null element must be returned as SQL NULL, with wasNull() reflecting it. */
  @Test
  public void getResultSet_NullElementShouldSetWasNull() throws SQLException {
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<STRING>"))
        .thenReturn("STRING");
    DatabricksArray array = new DatabricksArray(Arrays.asList("a", null), "ARRAY<STRING>");
    try (java.sql.ResultSet rs = array.getResultSet()) {
      assertTrue(rs.next());
      assertEquals("a", rs.getString(2));
      assertFalse(rs.wasNull());
      assertTrue(rs.next());
      assertEquals(2L, rs.getLong(1));
      assertNull(rs.getString(2));
      assertTrue(rs.wasNull());
      assertNull(rs.getObject(2));
      assertTrue(rs.wasNull());
      assertFalse(rs.next());
    }
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

  @Test
  public void testToString_WithStructElements_ShouldProduceJsonLikeString() throws SQLException {
    // Arrange
    String metadata = "ARRAY<STRUCT<age:INT,email:STRING>>";

    when(MetadataParser.parseArrayMetadata("ARRAY<STRUCT<age:INT,email:STRING>>"))
        .thenReturn("STRUCT<age:INT,email:STRING>");

    Map<String, String> structTypeMap = new LinkedHashMap<>();
    structTypeMap.put("age", "INT");
    structTypeMap.put("email", "STRING");
    when(MetadataParser.parseStructMetadata("STRUCT<age:INT,email:STRING>"))
        .thenReturn(structTypeMap);

    Map<String, Object> structData1 = new LinkedHashMap<>();
    structData1.put("age", 30);
    structData1.put("email", "john@example.com");

    Map<String, Object> structData2 = new LinkedHashMap<>();
    structData2.put("age", 40);
    structData2.put("email", "jane@example.com");

    List<Object> originalList = Arrays.asList(structData1, structData2);

    // Act
    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);
    String actual = databricksArray.toString();

    // Assert
    String expected =
        "[{\"age\":30,\"email\":\"john@example.com\"},{\"age\":40,\"email\":\"jane@example.com\"}]";
    assertEquals(
        expected,
        actual,
        "DatabricksArray.toString() should produce a JSON-like array of struct elements");
  }

  @Test
  public void testToString_WithMapElements_ShouldProduceJsonLikeString() throws SQLException {
    // Arrange
    String metadata = "ARRAY<MAP<STRING,INT>>";

    when(MetadataParser.parseArrayMetadata("ARRAY<MAP<STRING,INT>>")).thenReturn("MAP<STRING,INT>");

    when(MetadataParser.parseMapMetadataParts("MAP<STRING,INT>"))
        .thenReturn(new String[] {"STRING", "INT"});

    Map<String, Object> map1 = new LinkedHashMap<>();
    map1.put("key1", 10);
    map1.put("key2", 20);

    Map<String, Object> map2 = new LinkedHashMap<>();
    map2.put("key1", 30);
    map2.put("key2", 40);

    List<Object> originalList = Arrays.asList(map1, map2);

    // Act
    DatabricksArray databricksArray = new DatabricksArray(originalList, metadata);
    String actual = databricksArray.toString();

    // Assert
    String expected = "[{\"key1\":10,\"key2\":20},{\"key1\":30,\"key2\":40}]";
    assertEquals(
        expected,
        actual,
        "DatabricksArray.toString() should produce a JSON-like array of map elements with string keys quoted and int values unquoted");
  }

  @Test
  public void testToStringWithTimestamps() throws SQLException {
    // Test that timestamps in arrays are properly quoted
    java.sql.Timestamp ts1 = java.sql.Timestamp.valueOf("2024-01-01 12:30:45.123");
    java.sql.Timestamp ts2 = java.sql.Timestamp.valueOf("2024-02-15 08:15:30.456");

    String metadata = "ARRAY<TIMESTAMP>";
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<TIMESTAMP>"))
        .thenReturn("TIMESTAMP");

    DatabricksArray array = new DatabricksArray(List.of(ts1, ts2), metadata);
    String actual = array.toString();

    String expected = "[\"2024-01-01 12:30:45.123\",\"2024-02-15 08:15:30.456\"]";
    assertEquals(expected, actual, "DatabricksArray.toString() should quote timestamp elements");
  }

  @Test
  public void testToStringWithDates() throws SQLException {
    // Test that dates in arrays are properly quoted
    java.sql.Date date1 = java.sql.Date.valueOf("2024-01-01");
    java.sql.Date date2 = java.sql.Date.valueOf("2024-02-15");

    String metadata = "ARRAY<DATE>";
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata("ARRAY<DATE>"))
        .thenReturn("DATE");

    DatabricksArray array = new DatabricksArray(List.of(date1, date2), metadata);
    String actual = array.toString();

    String expected = "[\"2024-01-01\",\"2024-02-15\"]";
    assertEquals(expected, actual, "DatabricksArray.toString() should quote date elements");
  }

  @Test
  public void testToStringWithNestedStructContainingTimestamp() throws SQLException {
    // Test that timestamps in nested structures (array of struct with timestamp) are properly
    // quoted
    java.sql.Timestamp ts1 = java.sql.Timestamp.valueOf("2024-01-01 12:30:45.123");
    java.sql.Timestamp ts2 = java.sql.Timestamp.valueOf("2024-02-15 08:15:30.456");

    String structMetadata = "STRUCT<id:INT,created_at:TIMESTAMP>";
    Map<String, Object> fieldTypesMap = new LinkedHashMap<>();
    fieldTypesMap.put("id", "INT");
    fieldTypesMap.put("created_at", "TIMESTAMP");

    metadataParserMock
        .when(() -> MetadataParser.parseStructMetadata(structMetadata))
        .thenReturn(fieldTypesMap);

    Map<String, Object> struct1 = new LinkedHashMap<>();
    struct1.put("id", 1);
    struct1.put("created_at", ts1);

    Map<String, Object> struct2 = new LinkedHashMap<>();
    struct2.put("id", 2);
    struct2.put("created_at", ts2);

    DatabricksStruct databricksStruct1 = new DatabricksStruct(struct1, structMetadata);
    DatabricksStruct databricksStruct2 = new DatabricksStruct(struct2, structMetadata);

    String arrayMetadata = "ARRAY<STRUCT<id:INT,created_at:TIMESTAMP>>";
    metadataParserMock
        .when(() -> MetadataParser.parseArrayMetadata(arrayMetadata))
        .thenReturn(structMetadata);

    DatabricksArray array =
        new DatabricksArray(List.of(databricksStruct1, databricksStruct2), arrayMetadata);
    String actual = array.toString();

    String expected =
        "[{\"id\":1,\"created_at\":\"2024-01-01 12:30:45.123\"},{\"id\":2,\"created_at\":\"2024-02-15 08:15:30.456\"}]";
    assertEquals(
        expected, actual, "Nested structures should properly quote timestamps at all levels");
  }
}
