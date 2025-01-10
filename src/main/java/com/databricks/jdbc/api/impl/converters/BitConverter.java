package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

public class BitConverter implements ObjectConverter {
  private final IDatabricksConnectionContext connectionContext;

  public BitConverter(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
  }

  @Override
  public IDatabricksConnectionContext getConnectionContext() {
    return connectionContext;
  }

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
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION,
        getConnectionContext());
  }
}
