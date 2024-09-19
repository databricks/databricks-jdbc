package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

public abstract class AbstractObjectConverter {

  static long[] POWERS_OF_TEN = {
    1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000
  };

  /** Assumes that the object is in milliseconds. */
  static int DEFAULT_TIMESTAMP_SCALE = 3;

  Object object;

  AbstractObjectConverter(Object object) throws DatabricksSQLException {
    this.object = object;
  }

  public byte convertToByte() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public short convertToShort() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public int convertToInt() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public long convertToLong() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public float convertToFloat() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public double convertToDouble() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public LocalDate convertToLocalDate() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public boolean convertToBoolean() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public byte[] convertToByteArray() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public char convertToChar() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public String convertToString() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public Timestamp convertToTimestamp() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public Timestamp convertToTimestamp(int scale) throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public Date convertToDate() throws DatabricksSQLException {
    throw new DatabricksSQLException("Unsupported conversion operation");
  }

  public InputStream convertToBinaryStream() throws DatabricksSQLException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos)) {
      out.writeObject(object);
      out.flush();
      byte[] objectBytes = bos.toByteArray();
      return new ByteArrayInputStream(objectBytes);
    } catch (IOException e) {
      throw new DatabricksSQLException(
          "Could not convert object to binary stream " + object.toString(), e);
    }
  }

  public InputStream convertToUnicodeStream() throws DatabricksSQLException {
    String stringRepresentation = this.convertToString();
    return new ByteArrayInputStream(stringRepresentation.getBytes(StandardCharsets.UTF_8));
  }

  public InputStream convertToAsciiStream() throws DatabricksSQLException {
    String stringRepresentation = this.convertToString();
    byte[] asciiBytes = stringRepresentation.getBytes(StandardCharsets.US_ASCII);
    return new ByteArrayInputStream(asciiBytes);
  }

  public Reader convertToCharacterStream() throws DatabricksSQLException {
    String stringRepresentation = this.convertToString();
    return new StringReader(stringRepresentation);
  }
}
