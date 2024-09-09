package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;

public class BitConverter extends AbstractObjectConverter {
  public BitConverter(Object object) throws DatabricksSQLException {
    super(object);
  }

  public Boolean convertToBit() throws DatabricksSQLException {
    if (object instanceof Boolean) {
      return (Boolean) object;
    } else if (object instanceof Number) {
      return ((Number) object).intValue() != 0;
    } else if (object instanceof String) {
      return Boolean.parseBoolean((String) object);
    }
    throw new DatabricksSQLException(
        "Unsupported type for conversion to BIT: " + object.getClass());
  }
}
