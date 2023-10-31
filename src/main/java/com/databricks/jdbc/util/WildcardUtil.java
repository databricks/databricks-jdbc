package com.databricks.jdbc.util;

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

  /**
   * This function checks if the input string is a wildcard string
   *
   * @param s the input string
   * @return true if the input string is wildcard
   */
  public static boolean isWildcard(String s) {
    return s.contains(ASTERISK);
  }

  /**
   * This function checks if the input string contains an emoji
   *
   * @param str the input string
   * @return true if the input string contains an emoji
   */
  public static boolean containsEmoji(String str) {
    int length = str.length();
    for (int i = 0; i < length; i++) {
      int type = Character.getType(str.charAt(i));
      if (type == Character.SURROGATE || type == Character.OTHER_SYMBOL) {
        return true;
      }
    }
    return false;
  }
}
