package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

public abstract class AbstractObjectConverter {

  // TODO (Madhav): Ensure proper handling of null values in the conversions.
  long[] POWERS_OF_TEN = {
    1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000
  };

  int DEFAULT_TIMESTAMP_SCALE = 3;
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
}
