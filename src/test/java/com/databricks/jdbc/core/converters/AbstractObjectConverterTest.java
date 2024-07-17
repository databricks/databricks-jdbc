package com.databricks.jdbc.core.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import org.junit.jupiter.api.Test;

public class AbstractObjectConverterTest {

  static class TestableAbstractObjectConverter extends AbstractObjectConverter {
    TestableAbstractObjectConverter(Object object) throws DatabricksSQLException {
      super(object);
    }
  }

  @Test
  void testUnsupportedOperations() throws DatabricksSQLException {
    AbstractObjectConverter objectConverter = new TestableAbstractObjectConverter("testString");
    assertThrows(DatabricksSQLException.class, objectConverter::convertToByte);
    assertThrows(DatabricksSQLException.class, objectConverter::convertToShort);
    assertThrows(DatabricksSQLException.class, objectConverter::convertToInt);
    assertThrows(DatabricksSQLException.class, objectConverter::convertToBoolean);
    assertThrows(DatabricksSQLException.class, objectConverter::convertToLong);
    assertThrows(DatabricksSQLException.class, objectConverter::convertToFloat);
    assertThrows(DatabricksSQLException.class, objectConverter::convertToDouble);
    assertThrows(DatabricksSQLException.class, objectConverter::convertToBigDecimal);
    assertThrows(DatabricksSQLException.class, objectConverter::convertToByteArray);
    assertThrows(DatabricksSQLException.class, objectConverter::convertToChar);
    assertThrows(DatabricksSQLException.class, objectConverter::convertToString);
    assertThrows(DatabricksSQLException.class, objectConverter::convertToTimestamp);
    assertThrows(DatabricksSQLException.class, objectConverter::convertToDate);
    assertThrows(DatabricksSQLException.class, () -> objectConverter.convertToTimestamp(10));
    assertThrows(DatabricksSQLException.class, objectConverter::convertToBigInteger);
    assertThrows(DatabricksSQLException.class, objectConverter::convertToLocalDate);
  }

  @Test
  void testConvertToBinaryStream() throws Exception {
    String testString = "testString";
    TestableAbstractObjectConverter objectConverter =
        new TestableAbstractObjectConverter(testString);
    InputStream inputStream = objectConverter.convertToBinaryStream();
    assertNotNull(inputStream, "InputStream should not be null");
    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
    Object deserializedObject = objectInputStream.readObject();

    assertTrue(deserializedObject instanceof String, "Deserialized object should be a String");
    assertEquals(testString, deserializedObject, "Deserialized object should match the original");
  }

  @Test
  void testConvertToBinaryStreamWithException() throws DatabricksSQLException {
    Object nonSerializableObject =
        new Object() {
          private void writeObject(java.io.ObjectOutputStream out) throws IOException {
            throw new IOException("Serialization failed");
          }
        };
    TestableAbstractObjectConverter objectConverter =
        new TestableAbstractObjectConverter(nonSerializableObject);

    assertThrows(DatabricksSQLException.class, objectConverter::convertToBinaryStream);
  }
}
