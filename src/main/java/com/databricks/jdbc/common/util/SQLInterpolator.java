package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.MetadataResultConstants.NULL_STRING;

import com.databricks.jdbc.api.impl.ImmutableSqlParameter;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLInterpolator {
  protected static String escapeInputs(String input) {
    if (input == null) return null;
    StringBuilder out = new StringBuilder(input.length() + 16);
    out.append("'");
    for (int i = 0; i < input.length(); ) {
      int codePoint = input.codePointAt(i);
      i += Character.charCount(codePoint);
      switch (codePoint) {
        case '\'':
          out.append("''");
          break; // SQL-standard quote escape
        case '\\':
          out.append("\\\\");
          break; // escape backslash
        case '\n':
          out.append("\\n");
          break;
        case '\r':
          out.append("\\r");
          break;
        case '\t':
          out.append("\\t");
          break;
        default:
          // Basic Multilingual Plane — fits in one UTF-16 char, append directly
          if (codePoint <= 0xFFFF) {
            out.append((char) codePoint);
          } else {
            // Supplementary plane (e.g. emoji) — render as \UXXXXXXXX for clarity and portability
            // Example: 🧱 (U+1F9F1) → \U0001F9F1
            out.append(String.format("\\U%08X", codePoint)); // emoji etc.
          }
      }
    }
    out.append("'");
    return out.toString();
  }

  private static String formatObject(ImmutableSqlParameter object) {
    if (object == null || object.value() == null) {
      return NULL_STRING;
    } else if (object.type() == ColumnInfoTypeName.BINARY) {
      // Don't wrap within quotes. Don't treat hex literals as string.
      return object.value().toString();
    } else if (object.value() instanceof String) {
      return escapeInputs(object.value().toString());
    } else if (object.type() == ColumnInfoTypeName.TIMESTAMP
        || object.type() == ColumnInfoTypeName.DATE) {
      // Timestamp and Date types need to be quoted as strings
      return "'" + (object.value().toString()) + "'";
    } else {
      return object.value().toString();
    }
  }

  private static int countPlaceholders(String sql) {
    int count = 0;
    for (char c : sql.toCharArray()) {
      if (c == '?') {
        count++;
      }
    }
    return count;
  }

  /**
   * Interpolates the given SQL string by replacing placeholders with the provided parameters.
   *
   * <p>This method splits the SQL string by placeholders (question marks) and replaces each
   * placeholder with the corresponding parameter from the provided map. The map keys are 1-based
   * indexes, aligning with the SQL parameter positions.
   *
   * @param sql the SQL string containing placeholders ('?') to be replaced.
   * @param params a map of parameters where the key is the 1-based index of the placeholder in the
   *     SQL string, and the value is the corresponding {@link ImmutableSqlParameter}.
   * @return the interpolated SQL string with placeholders replaced by the corresponding parameters.
   * @throws DatabricksValidationException if the number of placeholders in the SQL string does not
   *     match the number of parameters provided in the map.
   */
  public static String interpolateSQL(String sql, Map<Integer, ImmutableSqlParameter> params)
      throws DatabricksValidationException {
    String[] parts = sql.split("\\?");
    if (countPlaceholders(sql) != params.size()) {
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

  /**
   * Surrounds unquoted placeholders (?) with single quotes, preserving already quoted ones. This is
   * crucial for DESCRIBE QUERY commands as unquoted placeholders will cause a parse_syntax_error.
   */
  public static String surroundPlaceholdersWithQuotes(String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    // This pattern matches any '?' that is NOT already inside single quotes
    StringBuilder sb = new StringBuilder();
    Matcher m = Pattern.compile("(?<!')\\?(?!')").matcher(sql);
    while (m.find()) {
      m.appendReplacement(sb, "'?'");
    }
    m.appendTail(sb);
    return sb.toString();
  }
}
