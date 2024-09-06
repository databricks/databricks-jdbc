package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Base64;

public class ByteArrayConverter extends AbstractObjectConverter {

  private final byte[] object;

  public ByteArrayConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      this.object = Base64.getDecoder().decode((String) object);
    } else if (object instanceof byte[]) {
      this.object = (byte[]) object;
    } else if (object instanceof ByteBuffer) {
      ByteBuffer byteBuffer = (ByteBuffer) object;
      if (byteBuffer.hasArray()) {
        this.object = byteBuffer.array();
      } else {
        this.object = new byte[byteBuffer.remaining()];
        byteBuffer.get(this.object);
      }
    } else {
      throw new DatabricksSQLException(
          "Unsupported type for ByteArrayConverter : " + object.getClass());
    }
  }

  @Override
  public byte convertToByte() throws DatabricksSQLException {
    if (object.length > 0) {
      return object[0];
    } else {
      throw new DatabricksSQLException("ByteArray is empty, cannot convert to single byte");
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
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    throw new DatabricksSQLException("Conversion from byte[] to BigInteger is not supported");
  }

  @Override
  public boolean convertToBoolean() throws DatabricksSQLException {
    if (object.length > 0) {
      return object[0] != 0;
    } else {
      return false;
    }
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return object;
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return Base64.getEncoder().encodeToString(object);
  }
}
