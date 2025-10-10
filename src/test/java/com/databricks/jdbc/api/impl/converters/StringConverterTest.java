package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.DatabricksArray;
import com.databricks.jdbc.api.impl.DatabricksMap;
import com.databricks.jdbc.api.impl.DatabricksStruct;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class StringConverterTest {

  private final String NUMERICAL_STRING = "10";
  private final String NUMBERICAL_ZERO_STRING = "0";
  private final String CHARACTER_STRING = "ABC";
  private final String TIME_STAMP_STRING = "2023-09-10 00:00:00";

  private final String ALT_TIMESTAMP_STRING = "2025-03-18 12:08:31.552223-07:00";
  private final String ALT_TIMESTAMP_STRING_WITH_EXTRA_QUOTES =
      "\"2025-03-18 12:08:31.552223-07:00\"";
  private final String DATE_STRING = "2023-09-10";

  private final String DATE_STRING_WITH_EXTRA_QUOTES = "\"2023-09-10\"";

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    String singleCharacterString = "A";
    assertEquals(new StringConverter().toByte(singleCharacterString), (byte) 'A');

    DatabricksSQLException tooManyCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toByte(CHARACTER_STRING));
    assertTrue(tooManyCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new StringConverter().toShort(NUMERICAL_STRING), (short) 10);
    assertEquals(new StringConverter().toShort(NUMBERICAL_ZERO_STRING), (short) 0);

    String stringThatDoesNotFitInShort = "32768";
    DatabricksSQLException outOfRangeException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter().toShort(stringThatDoesNotFitInShort));
    assertTrue(outOfRangeException.getMessage().contains("Invalid conversion"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toShort(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new StringConverter().toInt(NUMERICAL_STRING), 10);
    assertEquals(new StringConverter().toInt(NUMBERICAL_ZERO_STRING), 0);

    String stringThatDoesNotFitInInt = "2147483648";
    DatabricksSQLException outOfRangeException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter().toInt(stringThatDoesNotFitInInt));
    assertTrue(outOfRangeException.getMessage().contains("Invalid conversion"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toInt(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new StringConverter().toLong(NUMERICAL_STRING), 10);
    assertEquals(new StringConverter().toLong(NUMBERICAL_ZERO_STRING), 0);

    String stringThatDoesNotFitInLong = "9223372036854775808";
    DatabricksSQLException outOfRangeException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter().toLong(stringThatDoesNotFitInLong));
    assertTrue(outOfRangeException.getMessage().contains("Invalid conversion"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toLong(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new StringConverter().toFloat(NUMERICAL_STRING), 10f);
    assertEquals(new StringConverter().toFloat(NUMBERICAL_ZERO_STRING), 0f);
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toFloat(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new StringConverter().toDouble(NUMERICAL_STRING), 10);
    assertEquals(new StringConverter().toDouble(NUMBERICAL_ZERO_STRING), 0);
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toDouble(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new StringConverter().toBigDecimal(NUMERICAL_STRING), new BigDecimal("10"));
    assertEquals(new StringConverter().toBigDecimal(NUMBERICAL_ZERO_STRING), new BigDecimal("0"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toLong(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new StringConverter().toBoolean("1"));
    assertFalse(new StringConverter().toBoolean(NUMBERICAL_ZERO_STRING));
    assertTrue(new StringConverter().toBoolean("true"));
    assertFalse(new StringConverter().toBoolean("false"));
    assertTrue(new StringConverter().toBoolean("TRUE"));
    assertFalse(new StringConverter().toBoolean("FALSE"));
    assertTrue(new StringConverter().toBoolean(CHARACTER_STRING));
    assertTrue(new StringConverter().toBoolean(NUMERICAL_STRING));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(
        new StringConverter().toByteArray(NUMERICAL_STRING), NUMERICAL_STRING.getBytes());
    assertArrayEquals(
        new StringConverter().toByteArray(NUMBERICAL_ZERO_STRING),
        NUMBERICAL_ZERO_STRING.getBytes());
    assertArrayEquals(
        new StringConverter().toByteArray(CHARACTER_STRING), CHARACTER_STRING.getBytes());
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    assertEquals(new StringConverter().toChar(NUMBERICAL_ZERO_STRING), '0');
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toChar(NUMERICAL_STRING));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new StringConverter().toString(NUMERICAL_STRING), "10");
    assertEquals(new StringConverter().toString(NUMBERICAL_ZERO_STRING), "0");
    assertEquals(new StringConverter().toString(CHARACTER_STRING), "ABC");
  }

  @Test
  public void testConvertToStringWithComplexTypes() throws DatabricksSQLException {
    DatabricksArray stringArray = new DatabricksArray(List.of("one", "two"), "ARRAY<STRING>");
    assertEquals("[\"one\",\"two\"]", new StringConverter().toString(stringArray));

    Map<String, Object> mapValues = new LinkedHashMap<>();
    mapValues.put("alpha", "beta");
    DatabricksMap<String, Object> databricksMap =
        new DatabricksMap<>(mapValues, "MAP<STRING,STRING>");
    assertEquals("{\"alpha\":\"beta\"}", new StringConverter().toString(databricksMap));

    Map<String, Object> structValues = new LinkedHashMap<>();
    structValues.put("name", "value");
    structValues.put("score", 5);
    DatabricksStruct databricksStruct =
        new DatabricksStruct(structValues, "STRUCT<name:STRING,score:INT>");
    assertEquals(
        "{\"name\":\"value\",\"score\":5}", new StringConverter().toString(databricksStruct));
  }

  @Test
  public void testConvertToStringFallback() throws DatabricksSQLException {
    class CustomType {
      @Override
      public String toString() {
        return "custom-string";
      }
    }

    assertEquals("custom-string", new StringConverter().toString(new CustomType()));
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException, ParseException {
    assertEquals(
        new StringConverter().toTimestamp(TIME_STAMP_STRING), Timestamp.valueOf(TIME_STAMP_STRING));
    assertEquals(
        new StringConverter().toTimestamp(ALT_TIMESTAMP_STRING_WITH_EXTRA_QUOTES),
        new Timestamp(
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSXXX")
                .parse(ALT_TIMESTAMP_STRING)
                .getTime()));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    assertEquals(new StringConverter().toDate(DATE_STRING), Date.valueOf(DATE_STRING));
    assertEquals(
        new StringConverter().toDate(DATE_STRING_WITH_EXTRA_QUOTES), Date.valueOf(DATE_STRING));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(
        new StringConverter().toBigInteger(NUMERICAL_STRING), new BigDecimal("10").toBigInteger());
    assertEquals(
        new StringConverter().toBigInteger(NUMBERICAL_ZERO_STRING),
        new BigDecimal("0").toBigInteger());
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter().toBigInteger(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToStringWithSqlArray() throws DatabricksSQLException, SQLException {
    // Test with mock SQL Array
    java.sql.Array mockArray = org.mockito.Mockito.mock(java.sql.Array.class);
    when(mockArray.getArray()).thenReturn(new String[] {"a", "b", "c"});

    assertEquals("[\"a\",\"b\",\"c\"]", new StringConverter().toString(mockArray));

    // Test with null array data
    when(mockArray.getArray()).thenReturn(null);
    assertEquals("null", new StringConverter().toString(mockArray));

    // Test SQLException handling
    when(mockArray.getArray()).thenThrow(new SQLException("Test exception"));
    assertThrows(
        DatabricksValidationException.class, () -> new StringConverter().toString(mockArray));
  }

  @Test
  public void testConvertToStringWithGenericStruct() throws DatabricksSQLException, SQLException {
    // Test with mock Struct that's not DatabricksStruct
    java.sql.Struct mockStruct = org.mockito.Mockito.mock(java.sql.Struct.class);
    when(mockStruct.getAttributes()).thenReturn(new Object[] {"value1", 42, null});

    assertEquals("{\"value1\",42,null}", new StringConverter().toString(mockStruct));

    // Test with null attributes
    when(mockStruct.getAttributes()).thenReturn(null);
    assertEquals("{}", new StringConverter().toString(mockStruct));

    // Test SQLException handling
    when(mockStruct.getAttributes()).thenThrow(new SQLException("Test exception"));
    assertThrows(
        DatabricksValidationException.class, () -> new StringConverter().toString(mockStruct));
  }

  @Test
  public void testConvertToStringWithGenericMap() throws DatabricksSQLException {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("key1", "value1");
    map.put("key2", 42);
    map.put("key3", null);

    assertEquals(
        "{\"key1\":\"value1\",\"key2\":42,\"key3\":null}", new StringConverter().toString(map));

    // Test empty map
    assertEquals("{}", new StringConverter().toString(new HashMap<>()));
  }

  @Test
  public void testConvertToStringWithGenericCollection() throws DatabricksSQLException {
    List<Object> list = Arrays.asList("item1", 42, null, true);

    assertEquals("[\"item1\",42,null,true]", new StringConverter().toString(list));

    // Test empty collection
    assertEquals("[]", new StringConverter().toString(new ArrayList<>()));
  }

  @Test
  public void testConvertToStringWithJavaArrays() throws DatabricksSQLException {
    // String array
    String[] stringArray = {"a", "b", "c"};
    assertEquals("[\"a\",\"b\",\"c\"]", new StringConverter().toString(stringArray));

    // Integer array
    int[] intArray = {1, 2, 3};
    assertEquals("[1,2,3]", new StringConverter().toString(intArray));

    // Object array with mixed types
    Object[] mixedArray = {"text", 42, null, true};
    assertEquals("[\"text\",42,null,true]", new StringConverter().toString(mixedArray));

    // Empty array
    String[] emptyArray = {};
    assertEquals("[]", new StringConverter().toString(emptyArray));
  }

  @Test
  public void testEscapeString() throws DatabricksSQLException {
    StringConverter converter = new StringConverter();
    // Access private method via reflection for testing
    try {
      java.lang.reflect.Method escapeMethod =
          StringConverter.class.getDeclaredMethod("escapeString", String.class);
      escapeMethod.setAccessible(true);

      assertEquals("test", escapeMethod.invoke(converter, "test"));
      assertEquals("test\\\"quote", escapeMethod.invoke(converter, "test\"quote"));
      assertEquals("test\\\\backslash", escapeMethod.invoke(converter, "test\\backslash"));
    } catch (Exception e) {
      fail("Failed to test escapeString method: " + e.getMessage());
    }
  }

  @Test
  public void testConvertToStringWithNestedStructures() throws DatabricksSQLException {
    // Test nested map in collection
    Map<String, String> nestedMap = new HashMap<>();
    nestedMap.put("nested", "value");
    List<Object> listWithMap = Arrays.asList("item", nestedMap);

    assertEquals("[\"item\",{\"nested\":\"value\"}]", new StringConverter().toString(listWithMap));

    // Test nested collection in map
    Map<String, Object> mapWithList = new HashMap<>();
    mapWithList.put("list", Arrays.asList("a", "b"));

    assertEquals("{\"list\":[\"a\",\"b\"]}", new StringConverter().toString(mapWithList));
  }
}
