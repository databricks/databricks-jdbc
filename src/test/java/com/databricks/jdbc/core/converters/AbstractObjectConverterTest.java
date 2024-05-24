package com.databricks.jdbc.core.converters;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.databricks.jdbc.core.DatabricksSQLException;
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
  }
}
