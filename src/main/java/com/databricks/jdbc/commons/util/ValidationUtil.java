package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.core.DatabricksSQLException;

public class ValidationUtil {
  public static void checkIfPositive(int number, String fieldName) throws DatabricksSQLException {
    // Todo : Add appropriate exception
    if (number < 0) {
      throw new DatabricksSQLException(
          String.format("Invalid input for %s, : %d", fieldName, number));
    }
  }
}
