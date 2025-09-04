package com.databricks.jdbc.common.util;

import com.databricks.jdbc.exception.DatabricksValidationException;
import java.sql.SQLException;

public class WrapperUtil {
  public static boolean isWrapperFor(Class<?> iface, Object object) {
    return iface.isInstance(object);
  }

  public static <T> T unwrap(Class<T> iface, Object object) throws SQLException {
    try {
      return iface.cast(object);
    } catch (Exception exception) {
      throw new DatabricksValidationException("Cannot unwrap object to class", exception);
    }
  }
}
