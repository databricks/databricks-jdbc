package com.databricks.jdbc.commons.util;

public class StringUtil {
  public static String getProcessedEscapeSequence(String sql) {
    // Replace JDBC escape sequences;
    // TODO : Check if some other escape sequence is required by us.
    sql =
        sql.replaceAll("\\{d '([0-9]{4}-[0-9]{2}-[0-9]{2})'\\}", "DATE '$1'") // DATE
            .replaceAll("\\{t '([0-9]{2}:[0-9]{2}:[0-9]{2})'\\}", "TIME '$1'") // TIME
            .replaceAll(
                "\\{ts '([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?)'\\}",
                "TIMESTAMP '$1'") // TIMESTAMP
            .replaceAll("\\{fn ([^}]*)\\}", "$1"); // JDBC function escape sequence
    return sql;
  }
}
