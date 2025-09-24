package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksSQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class BitConverterTest {

  private final BitConverter bitConverter = new BitConverter();

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testToStringWithBooleanPrimitives(boolean input) throws DatabricksSQLException {
    String expected = input ? "true" : "false";
    assertEquals(expected, bitConverter.toString(input));
  }

  @ParameterizedTest
  @CsvSource({"true, true", "false, false"})
  public void testToStringWithBooleanObjects(String input, String expected)
      throws DatabricksSQLException {
    Boolean booleanInput = Boolean.parseBoolean(input);
    assertEquals(expected, bitConverter.toString(booleanInput));
  }

  @ParameterizedTest
  @CsvSource({
    "null, null",
    "Object, Object",
    "int[], class [I",
    "String[], class [Ljava.lang.String;"
  })
  public void testToStringWithUnsupportedTypes(String typeDescription, String expectedInMessage) {
    Object input = getTestObject(typeDescription);

    DatabricksSQLException exception =
        assertThrows(DatabricksSQLException.class, () -> bitConverter.toString(input));

    assertTrue(exception.getMessage().contains("Unsupported type for conversion to String"));
    assertTrue(exception.getMessage().contains(expectedInMessage));
    assertEquals("UNSUPPORTED_OPERATION", exception.getSQLState());
  }

  @Test
  public void testToBooleanWithBooleanTrue() throws DatabricksSQLException {
    assertTrue(bitConverter.toBoolean(true));
  }

  @Test
  public void testToBooleanWithBooleanFalse() throws DatabricksSQLException {
    assertFalse(bitConverter.toBoolean(false));
  }

  @Test
  public void testToBooleanWithBooleanObject() throws DatabricksSQLException {
    Boolean trueObject = Boolean.TRUE;
    Boolean falseObject = Boolean.FALSE;

    assertTrue(bitConverter.toBoolean(trueObject));
    assertFalse(bitConverter.toBoolean(falseObject));
  }

  @ParameterizedTest
  @CsvSource({"0, false", "1, true", "-1, true", "42, true", "0.0, false", "3.14, true"})
  public void testToBooleanWithNumbers(String numberStr, boolean expected)
      throws DatabricksSQLException {
    Number number =
        numberStr.contains(".") ? Double.parseDouble(numberStr) : Integer.parseInt(numberStr);
    assertEquals(expected, bitConverter.toBoolean(number));
  }

  @ParameterizedTest
  @CsvSource({
    "true, true",
    "TRUE, true",
    "True, true",
    "false, false",
    "FALSE, false",
    "False, false",
    "anything else, false",
    "'', false",
    "1, false",
    "0, false"
  })
  public void testToBooleanWithStrings(String input, boolean expected)
      throws DatabricksSQLException {
    assertEquals(expected, bitConverter.toBoolean(input));
  }

  @ParameterizedTest
  @CsvSource({
    "null, null",
    "Object, Object",
    "int[], class [I",
    "String[], class [Ljava.lang.String;"
  })
  public void testToBooleanWithUnsupportedTypes(String typeDescription, String expectedInMessage) {
    Object input = getTestObject(typeDescription);

    DatabricksSQLException exception =
        assertThrows(DatabricksSQLException.class, () -> bitConverter.toBoolean(input));

    assertTrue(exception.getMessage().contains("Unsupported type for conversion to BIT"));
    assertTrue(exception.getMessage().contains(expectedInMessage));
    assertEquals("UNSUPPORTED_OPERATION", exception.getSQLState());
  }

  private Object getTestObject(String typeDescription) {
    switch (typeDescription) {
      case "null":
        return null;
      case "Object":
        return new Object();
      case "int[]":
        return new int[] {1, 2, 3};
      case "String[]":
        return new String[] {"a", "b"};
      default:
        throw new IllegalArgumentException("Unknown type: " + typeDescription);
    }
  }
}
