package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

public class ByteArrayConverter extends AbstractObjectConverter {

  private byte[] object;

  public ByteArrayConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      this.object = ((String) object).getBytes(StandardCharsets.UTF_8);
    } else if (object instanceof byte[]) {
      this.object = (byte[]) object;
    } else {
      throw new DatabricksSQLException("Unsupported type for ByteArrayConverter");
    }
  }

  @Override
  public byte convertToByte() throws DatabricksSQLException {
    if (this.object.length > 0) {
      return this.object[0];
    } else {
      throw new DatabricksSQLException("ByteArray is empty, cannot convert to single byte");
    }
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    throw new DatabricksSQLException("Conversion from byte[] to BigInteger is not supported");
  }

  @Override
  public boolean convertToBoolean() throws DatabricksSQLException {
    if (this.object.length > 0) {
      return this.object[0] != 0;
    } else {
      return false;
    }
  }

  @Override
  public short convertToShort() throws DatabricksSQLException {
    throw new DatabricksSQLException("Conversion from byte[] to short is not supported");
  }

  @Override
  public int convertToInt() throws DatabricksSQLException {
    throw new DatabricksSQLException("Conversion from byte[] to int is not supported");
  }

  @Override
  public long convertToLong() throws DatabricksSQLException {
    throw new DatabricksSQLException("Conversion from byte[] to long is not supported");
  }

  @Override
  public float convertToFloat() throws DatabricksSQLException {
    throw new DatabricksSQLException("Conversion from byte[] to float is not supported");
  }

  @Override
  public double convertToDouble() throws DatabricksSQLException {
    throw new DatabricksSQLException("Conversion from byte[] to double is not supported");
  }

  @Override
  public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
    throw new DatabricksSQLException("Conversion from byte[] to BigDecimal is not supported");
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return this.object;
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return new String(this.object, StandardCharsets.UTF_8);
  }
}
