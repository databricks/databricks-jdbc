package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Base64;
import org.junit.jupiter.api.Test;

public class ByteArrayConverterTest {

  private final ByteArrayConverter converter = new ByteArrayConverter();

  @Test
  void testConvertToByteArrayFromString() throws DatabricksSQLException {
    String testString = "Test";
    assertArrayEquals(Base64.getDecoder().decode(testString), converter.toByteArray(testString));
  }

  @Test
  void testConvertToString() throws DatabricksSQLException {
    String testString = "Test";
    assertEquals(testString, converter.toString(testString));
  }

  @Test
  void testConvertFromString() throws DatabricksSQLException {
    String testString = "Test";
    assertEquals(converter.toString(testString), testString);
  }

  @Test
  void testConvertToByte() throws DatabricksSQLException {
    byte[] byteArray = {5};
    assertEquals(5, converter.toByte(byteArray));
    assertThrows(DatabricksSQLException.class, () -> converter.toByte(new byte[] {}));
  }

  @Test
  public void testByteArrayConverterWithHeapByteBuffer() throws DatabricksSQLException {
    byte[] byteArray = {5, 6, 7, 8};
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
    assertArrayEquals(byteArray, converter.toByteArray(buffer));
  }

  @Test
  void testConvertToBoolean() throws DatabricksSQLException {
    assertThrows(DatabricksSQLException.class, () -> converter.toBoolean(new byte[] {1}));
    assertThrows(DatabricksSQLException.class, () -> converter.toBoolean(new byte[] {}));
  }

  @Test
  void testConvertThrowsException() {
    assertThrows(
        DatabricksSQLException.class,
        () -> converter.toByteArray(Timestamp.valueOf(LocalDateTime.now())));
  }

  @Test
  void testUnsupportedConversions() {
    ByteArrayConverter converter = new ByteArrayConverter();
    assertAll(
        "Unsupported Conversions",
        () ->
            assertThrows(
                DatabricksSQLException.class,
                () -> converter.toShort(new byte[] {}),
                "Short conversion should throw exception"),
        () ->
            assertThrows(
                DatabricksSQLException.class,
                () -> converter.toInt(new byte[] {}),
                "Int conversion should throw exception"),
        () ->
            assertThrows(
                DatabricksSQLException.class,
                () -> converter.toLong(new byte[] {}),
                "Long conversion should throw exception"),
        () ->
            assertThrows(
                DatabricksSQLException.class,
                () -> converter.toFloat(new byte[] {}),
                "Float conversion should throw exception"),
        () ->
            assertThrows(
                DatabricksSQLException.class,
                () -> converter.toDouble(new byte[] {}),
                "Double conversion should throw exception"),
        () ->
            assertThrows(
                DatabricksSQLException.class,
                () -> converter.toBigDecimal(new byte[] {}),
                "BigDecimal conversion should throw exception"));
  }

  @Test
  void testConvertToBigInteger() {
    assertThrows(DatabricksSQLException.class, () -> converter.toBigInteger(new byte[] {}));
  }
}
