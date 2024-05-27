package com.databricks.jdbc.commons.util;

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

  /**
   * This function checks if the input string is a wildcard string
   *
   * @param s the input string
   * @return true if the input string is wildcard
   */
  public static boolean isWildcard(String s) {
    return s != null && s.contains(ASTERISK);
  }

  public static String jdbcPatternToHive(String pattern) {
    if (pattern == null) {
      return null;
    }
    pattern = pattern.replaceAll("\\\\_", "_");
    pattern = pattern.replaceAll("\\\\%", "%");
    return pattern;
  }
}
