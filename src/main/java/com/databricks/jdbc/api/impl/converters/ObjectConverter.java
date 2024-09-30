package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

public interface ObjectConverter {
  long[] POWERS_OF_TEN = {
    1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000
  };
  int DEFAULT_TIMESTAMP_SCALE = 3;

  default byte toByte(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported byte conversion operation");
  }

  default short toShort(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported short conversion operation");
  }

  default int toInt(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported int conversion operation");
  }

  default long toLong(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported long conversion operation");
  }

  default float toFloat(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported float conversion operation");
  }

  default double toDouble(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported double conversion operation");
  }

  default BigDecimal toBigDecimal(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported BigDecimal conversion operation");
  }

  default BigDecimal toBigDecimal(Object object, int scale) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported BigDecimal(scale) conversion operation");
  }

  default BigInteger toBigInteger(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported BigInteger conversion operation");
  }

  default LocalDate toLocalDate(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported LocalDate conversion operation");
  }

  default boolean toBoolean(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported boolean conversion operation");
  }

  default byte[] toByteArray(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported byte[] conversion operation");
  }

  default char toChar(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported char conversion operation");
  }

  default String toString(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported String conversion operation");
  }

  default Timestamp toTimestamp(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported Timestamp conversion operation");
  }

  default Timestamp toTimestamp(Object object, int scale) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported Timestamp(scale) conversion operation");
  }

  default Date toDate(Object object) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported Date conversion operation");
  }

  default InputStream toBinaryStream(Object object) throws DatabricksSQLException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      objectOutputStream.writeObject(object);
      objectOutputStream.flush();
      return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    } catch (IOException e) {
      throw new DatabricksSQLException(
          "Could not convert object to binary stream " + object.toString(), e);
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
