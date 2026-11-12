package com.databricks.jdbc.common.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StringUtil {
  private static final String ESCAPE_KEYWORD = "ESCAPE";

  // Whitespace + ESCAPE + whitespace + empty string ('') or (""), followed by a delimiter
  // (whitespace, semicolon, end-of-string, closing paren, or comma).
  private static final Pattern REDUNDANT_ESCAPE_CLAUSE =
      Pattern.compile("(?i)\\s+ESCAPE\\s+['\"]['\"](?=\\s|;|$|\\)|,)");

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

  public static List<String> split(String value) {
    if (value == null) {
      return Collections.emptyList();
    }
    return List.of(value.split(","));
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

  /**
   * Removes empty ESCAPE clauses from SQL statements to work around Databricks SQL parser
   * limitations.
   *
   * <p>All other ESCAPE clauses, including custom escape characters (e.g., {@code ESCAPE '#'}), are
   * preserved unchanged.
   *
   * @param sql The SQL statement to process
   * @return SQL with empty ESCAPE clauses removed, or null if input is null
   */
  public static String removeRedundantEscapeClause(String sql) {
    if (sql == null) {
      return null;
    }
    // The regex is slow on large statements, and most statements have no ESCAPE keyword.
    if (!containsIgnoreCase(sql, ESCAPE_KEYWORD)) {
      return sql;
    }
    return REDUNDANT_ESCAPE_CLAUSE.matcher(sql).replaceAll("");
  }

  /** ASCII case-insensitive search; {@code word} must be upper-case ASCII letters. */
  private static boolean containsIgnoreCase(String text, String word) {
    char first = word.charAt(0);
    char firstLower = Character.toLowerCase(first);
    int last = text.length() - word.length();
    for (int i = 0; i <= last; i++) {
      char c = text.charAt(i);
      if ((c == first || c == firstLower) && text.regionMatches(true, i, word, 0, word.length())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Parses a comma-separated string of integers into a Set.
   *
   * @param input comma-separated string of integers (e.g., "500, 503, 504")
   * @return Set of parsed integers, or empty set if input is null/empty or all values are invalid
   * @throws NumberFormatException if any value cannot be parsed as an integer (after validation)
   */
  public static Set<Integer> parseIntegerSet(String input) {
    if (input.trim().isEmpty()) {
      return Collections.emptySet();
    }
    return Arrays.stream(input.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .filter(num -> num.matches("\\d+")) // Ensure only positive integers
        .map(Integer::parseInt)
        .collect(Collectors.toSet());
  }
}
