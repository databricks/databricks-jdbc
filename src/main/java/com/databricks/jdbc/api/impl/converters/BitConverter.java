package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

public class BitConverter implements ObjectConverter {

  @Override
  public boolean toBoolean(Object object) throws DatabricksSQLException {
    if (object instanceof Boolean) {
      return (Boolean) object;
    }
    if (object instanceof Number) {
      return ((Number) object).intValue() != 0;
    }
    if (object instanceof String) {
      return Boolean.parseBoolean((String) object);
    }
    throw new DatabricksSQLException(
        "Unsupported type for conversion to BIT: " + object.getClass(),
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    if (object instanceof Boolean) {
      return object.toString();
    }
    // For other types, fall back to the default behavior
    throw new DatabricksSQLException(
        "Unsupported String conversion operation", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }
}
