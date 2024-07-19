package com.databricks.jdbc.core.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

class ByteArrayConverterTest {
  @Test
  void testConvertToByteArrayFromString() throws DatabricksSQLException {
    String testString = "Test";
    ByteArrayConverter converter = new ByteArrayConverter(testString);
    assertArrayEquals(testString.getBytes(StandardCharsets.UTF_8), converter.convertToByteArray());
  }

  @Test
  void testConvertToString() throws DatabricksSQLException {
    String testString = "Test";
    ByteArrayConverter converter =
        new ByteArrayConverter(testString.getBytes(StandardCharsets.UTF_8));
    assertEquals(testString, converter.convertToString());
  }

  @Test
  void testConvertFromString() throws DatabricksSQLException {
    String testString = "Test";
    ByteArrayConverter converter = new ByteArrayConverter(testString);
    assertEquals(converter.convertToString(), testString);
  }

  @Test
  void testConvertToByte() throws DatabricksSQLException {
    byte[] byteArray = {5};
    ByteArrayConverter converter = new ByteArrayConverter(byteArray);
    assertEquals(5, converter.convertToByte());
    assertThrows(
        DatabricksSQLException.class, () -> new ByteArrayConverter(new byte[] {}).convertToByte());
  }

  @Test
  public void testByteArrayConverterWithHeapByteBuffer() throws DatabricksSQLException {
    byte[] byteArray = {5, 6, 7, 8};
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
    ByteArrayConverter converter = new ByteArrayConverter(buffer);
    assertArrayEquals(byteArray, converter.convertToByteArray());
  }

  @Test
  void testConvertToBooleanTrue() throws DatabricksSQLException {
    byte[] byteArray = {1};
    ByteArrayConverter converter = new ByteArrayConverter(byteArray);
    assertTrue(converter.convertToBoolean());
    byteArray = new byte[] {};
    converter = new ByteArrayConverter(byteArray);
    assertFalse(converter.convertToBoolean());
  }

  @Test
  void testConvertToBooleanFalse() throws DatabricksSQLException {
    byte[] byteArray = {0};
    ByteArrayConverter converter = new ByteArrayConverter(byteArray);
    assertFalse(converter.convertToBoolean());
  }

  @Test
  void testConvertThrowsException() {
    assertThrows(
        DatabricksSQLException.class,
        () -> new ByteArrayConverter(Timestamp.valueOf(LocalDateTime.now())));
  }

  @Test
  void testUnsupportedConversions() throws DatabricksSQLException {
    ByteArrayConverter converter = new ByteArrayConverter(new byte[] {});
    assertAll(
        "Unsupported Conversions",
        () ->
            assertThrows(
                DatabricksSQLException.class,
                converter::convertToShort,
                "Short conversion should throw exception"),
        () ->
            assertThrows(
                DatabricksSQLException.class,
                converter::convertToInt,
                "Int conversion should throw exception"),
        () ->
            assertThrows(
                DatabricksSQLException.class,
                converter::convertToLong,
                "Long conversion should throw exception"),
        () ->
            assertThrows(
                DatabricksSQLException.class,
                converter::convertToFloat,
                "Float conversion should throw exception"),
        () ->
            assertThrows(
                DatabricksSQLException.class,
                converter::convertToDouble,
                "Double conversion should throw exception"),
        () ->
            assertThrows(
                DatabricksSQLException.class,
                converter::convertToBigDecimal,
                "BigDecimal conversion should throw exception"));
  }

  @Test
  public void testConvertToBigInteger() {
    assertThrows(
        DatabricksSQLException.class,
        () -> new ByteArrayConverter(new byte[] {}).convertToBigInteger());
  }
}
