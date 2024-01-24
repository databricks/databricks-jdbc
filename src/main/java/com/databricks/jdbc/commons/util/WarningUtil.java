package com.databricks.jdbc.commons.util;

import java.sql.SQLWarning;

public class WarningUtil {
  public static SQLWarning addWarning(SQLWarning warning, String warningText) {
    SQLWarning newWarning = new SQLWarning(warningText);
    if (warning == null) {
      return newWarning;
    }
    warning.setNextWarning(newWarning);
    return warning;
  }
}
