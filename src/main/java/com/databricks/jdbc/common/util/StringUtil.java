package com.databricks.jdbc.common.util;

public class StringUtil {
  public static String convertJdbcEscapeSequences(String sql) {
    // Replace JDBC escape sequences;
    sql =
        sql.replaceAll("\\{d '([0-9]{4}-[0-9]{2}-[0-9]{2})'\\}", "DATE '$1'") // DATE
            .replaceAll("\\{t '([0-9]{2}:[0-9]{2}:[0-9]{2})'\\}", "TIME '$1'") // TIME
            .replaceAll(
                "\\{ts '([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?)'\\}",
                "TIMESTAMP '$1'") // TIMESTAMP
            .replaceAll("\\{fn ([^}]*)\\}", "$1") // JDBC function escape sequence
            .replaceAll("\\{oj ([^}]*)\\}", "$1") // OUTER JOIN escape sequence
            .replaceAll("\\{call ([^}]*)\\}", "CALL $1"); // Stored Procedure escape sequence
    return sql;
  }

  /**
   * Escape sql string literal which is enclosed in a single quote, it replaces single quote with
   * doubled single quotes.
   *
   * <p>Please always use prepareStatement to bind variables if possible, only use it when
   * prepareStatement is not applicable, e.g. some DDL statement
   */
  public static String escapeStringLiteral(String str) {
    if (str == null) {
      return null;
    }
    return str.replace("'", "''");
  }
}
