package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.nio.ByteBuffer;
import java.util.Base64;

public class ByteArrayConverter implements ObjectConverter {
  @Override
  public byte[] toByteArray(Object object) throws DatabricksSQLException {
    if (object instanceof String) {
      return Base64.getDecoder().decode((String) object);
    } else if (object instanceof byte[]) {
      return (byte[]) object;
    } else if (object instanceof ByteBuffer) {
      ByteBuffer byteBuffer = (ByteBuffer) object;
      if (byteBuffer.hasArray()) {
        return byteBuffer.array();
      } else {
        byte[] result = new byte[byteBuffer.remaining()];
        byteBuffer.get(result);
        return result;
      }
    } else {
      throw new DatabricksSQLException(
          "Unsupported type for ByteArrayObjectConverter : " + object.getClass(),
          DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
    }
  }

  @Override
  public byte toByte(Object object) throws DatabricksSQLException {
    byte[] byteArray = toByteArray(object);
    if (byteArray.length > 0) {
      return byteArray[0];
    } else {
      throw new DatabricksValidationException("ByteArray is empty, cannot convert to single byte");
    }
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    return Base64.getEncoder().encodeToString(toByteArray(object));
  }
}
