package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.core.DatabricksValidationException;
import com.databricks.jdbc.core.ImmutableSqlParameter;
import java.util.Map;

import static com.databricks.jdbc.client.impl.helper.MetadataResultConstants.NULL_STRING;

public class SQLInterpolator {
  private static String formatObject(ImmutableSqlParameter object) {
   if(object == null || object.value() == null){
     return NULL_STRING;
   }
   else if (object.value() instanceof String) {
      return "'" + object.value() + "'";
    } else {
      return object.value().toString();
    }
  }

  public static String interpolateSQL(String sql, Map<Integer, ImmutableSqlParameter> params)
      throws DatabricksValidationException {
    String[] parts = sql.split("\\?");
    if (parts.length != params.size()) {
      throw new DatabricksValidationException(
          "Parameter count does not match. Provide equal number of parameters as placeholders. SQL "
              + sql);
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < parts.length; i++) {
      sb.append(parts[i]);
      if (i < params.size()) {
        sb.append(formatObject(params.get(i + 1))); // because we have 1 based index in params
      }
    }
    return sb.toString();
  }
}
