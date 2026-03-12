package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import org.junit.jupiter.api.Test;

public class ObjectConverterTest {

  static class TestableObjectConverter implements ObjectConverter {}

  @Test
  void testUnsupportedOperations() {
    ObjectConverter objectConverter = new ObjectConverterTest.TestableObjectConverter();
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toByte("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toShort("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toInt("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toBoolean("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toLong("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toFloat("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toDouble("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toBigDecimal("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toByteArray("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toChar("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toString("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toTime("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toTimestamp("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toDate("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toTimestamp("testString", 10));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toBigInteger("testString"));
    assertThrows(DatabricksSQLException.class, () -> objectConverter.toLocalDate("testString"));
  }

  @Test
  void testConvertToBinaryStream() throws Exception {
    String testString = "testString";
    ObjectConverter objectConverter = new ObjectConverterTest.TestableObjectConverter();
    InputStream inputStream = objectConverter.toBinaryStream("testString");
    assertNotNull(inputStream, "InputStream should not be null");
    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
    Object deserializedObject = objectInputStream.readObject();

    assertInstanceOf(String.class, deserializedObject, "Deserialized object should be a String");
    assertEquals(testString, deserializedObject, "Deserialized object should match the original");
  }

  @Test
  void testConvertToBinaryStreamWithException() {
    Object nonSerializableObject =
        new Object() {
          private void writeObject(java.io.ObjectOutputStream out) throws IOException {
            throw new IOException("Serialization failed");
          }
        };
    ObjectConverter objectConverter = new ObjectConverterTest.TestableObjectConverter();

    assertThrows(
        DatabricksSQLException.class, () -> objectConverter.toBinaryStream(nonSerializableObject));
  }
}
