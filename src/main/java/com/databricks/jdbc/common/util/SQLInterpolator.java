package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.MetadataResultConstants.NULL_STRING;

import com.databricks.jdbc.api.impl.ImmutableSqlParameter;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import java.util.Map;
import java.util.regex.Pattern;

public class SQLInterpolator {

  // Hex literal emitted by setBytes (X'<hex>'); only this shape is spliced unquoted (SEC-20590).
  private static final Pattern HEX_LITERAL_PATTERN = Pattern.compile("X'[0-9A-Fa-f]*'");

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
      // Splice raw only if it's a genuine hex literal; otherwise escape to avoid SQL injection.
      String binaryValue = object.value().toString();
      if (HEX_LITERAL_PATTERN.matcher(binaryValue).matches()) {
        return binaryValue;
      }
      return escapeInputs(binaryValue);
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

  /**
   * Interpolates the given SQL string by replacing placeholders with the provided parameters.
   *
   * <p>Only '?' characters that appear outside of comments, string literals, and quoted identifiers
   * are treated as placeholders. The map keys are 1-based indexes, aligning with the SQL parameter
   * positions.
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
    int[] positions = SqlCommentParser.findPlaceholderPositions(sql);
    if (positions.length != params.size()) {
      throw new DatabricksValidationException(
          "Parameter count does not match. Provide equal number of parameters as placeholders. SQL "
              + sql);
    }
    StringBuilder sb = new StringBuilder(sql.length() + 16);
    int last = 0;
    for (int i = 0; i < positions.length; i++) {
      int pos = positions[i];
      sb.append(sql, last, pos);
      sb.append(formatObject(params.get(i + 1))); // 1-based index in params
      last = pos + 1;
    }
    if (last < sql.length()) {
      sb.append(sql, last, sql.length());
    }
    return sb.toString();
  }

  /**
   * Surrounds real placeholders (?) with single quotes, leaving '?' characters that appear inside
   * comments, string literals, or quoted identifiers untouched. This is crucial for DESCRIBE QUERY
   * commands, where bare placeholders cause a parse_syntax_error.
   */
  public static String surroundPlaceholdersWithQuotes(String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    int[] positions = SqlCommentParser.findPlaceholderPositions(sql);
    if (positions.length == 0) {
      return sql;
    }
    StringBuilder sb = new StringBuilder(sql.length() + positions.length * 2);
    int last = 0;
    for (int pos : positions) {
      sb.append(sql, last, pos).append("'?'");
      last = pos + 1;
    }
    if (last < sql.length()) {
      sb.append(sql, last, sql.length());
    }
    return sb.toString();
  }
}
