package com.databricks.jdbc.common.util;

/**
 * This class consists of utility functions with respect to wildcard strings that are required in
 * building SQL queries
 */
public class WildcardUtil {
  private static final String ASTERISK = "*";

  /**
   * This function checks if the input string is a "match anything" string i.e. "*"
   *
   * @param s the input string
   * @return true if the input string is "*"
   */
  public static boolean isMatchAnything(String s) {
    return ASTERISK.equals(s);
  }

  public static boolean isNullOrEmpty(String s) {
    return s == null || s.trim().isEmpty();
  }

  public static boolean isNullOrWildcard(String s) {
    return s == null || isWildcard(s) || s.equals("%");
  }

  /**
   * This function checks if the input string is a wildcard string
   *
   * @param s the input string
   * @return true if the input string is wildcard
   */
  public static boolean isWildcard(String s) {
    return s != null && s.equals(ASTERISK);
  }

  public static String jdbcPatternToHive(String pattern) {
    if (pattern == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    boolean escapeNext = false; // Flag to check if the next character is escaped
    for (int i = 0; i < pattern.length(); i++) {
      char ch = pattern.charAt(i);

      if (ch == '\\') {
        // Check if it's an escaped backslash
        if (i + 1 < pattern.length() && pattern.charAt(i + 1) == '\\') {
          builder.append("\\\\");
          i++; // Skip the next backslash since it's part of the escape sequence
        } else {
          escapeNext = !escapeNext; // Toggle escape state for next character
        }
      } else if (escapeNext) {
        // If the current character is escaped, add it directly
        builder.append(ch);
        escapeNext = false; // Reset escape state
      } else {
        // Handle unescaped wildcards
        switch (ch) {
          case '%':
            builder.append("*");
            break;
          case '_':
            builder.append(".");
            break;
          default:
            builder.append(ch);
            break;
        }
      }
    }
    return builder.toString();
  }
}
