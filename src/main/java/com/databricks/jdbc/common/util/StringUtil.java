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

  /** Function to check if the given prefix exists in the fileName */
  public static boolean checkPrefixMatch(String prefix, String fileName, boolean caseSensitive) {
    return prefix.isEmpty()
        || fileName.regionMatches(
            /* ignoreCase= */ !caseSensitive,
            /* targetOffset= */ 0,
            /* StringToCheck= */ prefix,
            /* sourceOffset= */ 0,
            /* lengthToMatch= */ prefix.length());
  }

  /** Function to return the folder name from the path */
  public static String getFolderNameFromPath(String path) {
    if (path == null) return "";
    int lastSlashIndex = path.lastIndexOf("/");
    return (lastSlashIndex >= 0) ? path.substring(0, lastSlashIndex) : "";
  }

  /** Function to return the base name from the path */
  public static String getBaseNameFromPath(String path) {
    if (path == null) return "";
    int lastSlashIndex = path.lastIndexOf("/");
    return (lastSlashIndex >= 0) ? path.substring(lastSlashIndex + 1) : path;
  }

  /** Building the volume path using the provided catalog, schema and volume */
  public static String getVolumePath(String catalog, String schema, String volume) {
    // We need to escape '' to prevent SQL injection
    return escapeStringLiteral(String.format("/Volumes/%s/%s/%s/", catalog, schema, volume));
  }
}
