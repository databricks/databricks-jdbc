package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.api.impl.DatabricksArray;
import com.databricks.jdbc.api.impl.DatabricksMap;
import com.databricks.jdbc.api.impl.DatabricksStruct;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;

public interface ObjectConverter {
  long[] POWERS_OF_TEN = {
    1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000
  };
  int DEFAULT_TIMESTAMP_SCALE = 3;

  default byte toByte(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported byte conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default short toShort(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported short conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default int toInt(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported int conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default long toLong(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported long conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default float toFloat(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported float conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default double toDouble(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported double conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default BigDecimal toBigDecimal(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported BigDecimal conversion operation",
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default BigDecimal toBigDecimal(Object object, int scale) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported BigDecimal(scale) conversion operation",
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default BigInteger toBigInteger(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported BigInteger conversion operation",
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default LocalDate toLocalDate(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported LocalDate conversion operation",
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default boolean toBoolean(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported boolean conversion operation",
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default byte[] toByteArray(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported byte[] conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default char toChar(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported char conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default String toString(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported String conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default Time toTime(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported Time conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default Timestamp toTimestamp(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported Timestamp conversion operation",
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default Timestamp toTimestamp(Object object, int scale) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported Timestamp(scale) conversion operation",
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default Date toDate(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException(
        "Unsupported Date conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default DatabricksArray toDatabricksArray(Object object) throws DatabricksSQLException {
    if (object instanceof DatabricksArray) {
      return (DatabricksArray) object;
    }
    throw new DatabricksSQLException(
        "Unsupported Array conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default DatabricksMap toDatabricksMap(Object object) throws DatabricksSQLException {
    if (object instanceof DatabricksMap) {
      return (DatabricksMap) object;
    }
    throw new DatabricksSQLException(
        "Unsupported Map conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default DatabricksStruct toDatabricksStruct(Object object) throws DatabricksSQLException {
    if (object instanceof DatabricksStruct) {
      return (DatabricksStruct) object;
    }
    throw new DatabricksSQLException(
        "Unsupported Struct conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  default InputStream toBinaryStream(Object object) throws DatabricksSQLException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutputStream.writeObject(object);
      objectOutputStream.flush();
      return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    } catch (IOException e) {
      throw new DatabricksSQLException(
          "Could not convert object to binary stream " + object.toString(),
          e,
          DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
    }
  }

  default InputStream toUnicodeStream(Object object) throws DatabricksSQLException {
    return new ByteArrayInputStream(toString(object).getBytes(StandardCharsets.UTF_8));
  }

  default InputStream toAsciiStream(Object object) throws DatabricksSQLException {
    return new ByteArrayInputStream(toString(object).getBytes(StandardCharsets.US_ASCII));
  }

  default Reader toCharacterStream(Object object) throws DatabricksSQLException {
    return new StringReader(toString(object));
  }
}
