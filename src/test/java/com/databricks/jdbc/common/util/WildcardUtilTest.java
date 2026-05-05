package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class WildcardUtilTest {
  private static WildcardUtil wildcardUtil = new WildcardUtil();

  private static Stream<Arguments> listPatterns() {
    return Stream.of(
        Arguments.of("abc", "abc", "Same string check"),
        Arguments.of("abc%", "abc*", "Replace % with *"),
        Arguments.of("%abc", "*abc", "Replace % with * check 2"),
        Arguments.of("%abc%", "*abc*", "Replace % with * check 3"),
        Arguments.of("abc_", "abc.", "Replace _ with ."),
        Arguments.of("_abc", ".abc", "Replace _ with . check 2"),
        Arguments.of("_abc_", ".abc.", "Replace _ with . check 3"),
        Arguments.of("abc__", "abc..", "Replace _ with . check 4"),
        Arguments.of("__abc", "..abc", "Replace _ with . check 5"),
        Arguments.of("abc\\%", "abc%", "Escape character check"),
        Arguments.of("abc\\_", "abc_", "Escape character check 2"),
        Arguments.of("abc\\_def", "abc_def", "Escape character check 3"),
        Arguments.of("abc\\\\", "abc\\\\", "Escape character check 4"),
        Arguments.of("abc\\\\_", "abc\\\\.", "Escape character check 5"));
  }

  @ParameterizedTest
  @MethodSource("listPatterns")
  public void testJDBCToHiveConversion(
      String inputPattern, String expectedOutput, String errorMessage) {
    String actualOutput = wildcardUtil.jdbcPatternToHive(inputPattern);
    assertEquals(expectedOutput, actualOutput, errorMessage);
  }

  @Test
  public void testIsWildcard() {
    assertTrue(wildcardUtil.isWildcard("*"));
    assertFalse(wildcardUtil.isWildcard("*Test*"));
    assertFalse(wildcardUtil.isWildcard("Test"));
    assertFalse(wildcardUtil.isWildcard(null));
  }

  @Test
  public void testIsNullOrEmptyWithWhitespace() {
    assertFalse(wildcardUtil.isNullOrEmpty("Test"));
    assertTrue(wildcardUtil.isNullOrEmpty(null));
    assertTrue(wildcardUtil.isNullOrEmpty(""));
    assertTrue(wildcardUtil.isNullOrEmpty("    "));
  }

  @Test
  void testIsMatchAnything() {
    assertTrue(wildcardUtil.isMatchAnything("*"));
    assertFalse(wildcardUtil.isMatchAnything("Test"));
    assertFalse(wildcardUtil.isMatchAnything(null));
  }

  @Test
  void testIsNullOrWildcard() {
    assertTrue(WildcardUtil.isNullOrWildcard(null));
    assertTrue(WildcardUtil.isNullOrWildcard("*"));
    assertTrue(WildcardUtil.isNullOrWildcard("%"));
    assertFalse(WildcardUtil.isNullOrWildcard("test"));
    assertFalse(WildcardUtil.isNullOrWildcard(""));
    assertFalse(WildcardUtil.isNullOrWildcard("abc%"));
  }

  @Test
  void testJdbcPatternToHiveNull() {
    assertNull(WildcardUtil.jdbcPatternToHive(null));
  }

  private static Stream<Arguments> escapeCatalogNamePatterns() {
    return Stream.of(
        Arguments.of(null, null, "Null input returns null"),
        Arguments.of("simple", "simple", "No wildcards unchanged"),
        Arguments.of("my_catalog", "my\\_catalog", "Underscore is escaped"),
        Arguments.of("a_b_c", "a\\_b\\_c", "Multiple underscores escaped"),
        Arguments.of("my\\_catalog", "my\\_catalog", "Already escaped underscore unchanged"),
        Arguments.of("my%catalog", "my%catalog", "Percent is not escaped"),
        Arguments.of("a_b%c", "a\\_b%c", "Underscore escaped but percent left unchanged"));
  }

  @ParameterizedTest
  @MethodSource("escapeCatalogNamePatterns")
  void testEscapeCatalogName(String input, String expected, String errorMessage) {
    assertEquals(expected, WildcardUtil.escapeCatalogName(input), errorMessage);
  }

  private static Stream<Arguments> stripJdbcEscapesPatterns() {
    return Stream.of(
        Arguments.of(null, null, "Null input returns null"),
        Arguments.of("simple", "simple", "No escapes unchanged"),
        Arguments.of(
            "comparator\\_tests", "comparator_tests", "Escaped underscore becomes literal"),
        Arguments.of("a\\_b\\_c", "a_b_c", "Multiple escaped underscores"),
        Arguments.of("comparator_tests", "comparator_tests", "Unescaped underscore unchanged"),
        Arguments.of("abc\\\\", "abc\\", "Escaped backslash becomes single backslash"),
        Arguments.of("abc\\%", "abc%", "Escaped percent becomes literal"),
        Arguments.of("no\\_escape\\%here", "no_escape%here", "Mixed escapes stripped"),
        Arguments.of("", "", "Empty string returns empty"));
  }

  @ParameterizedTest
  @MethodSource("stripJdbcEscapesPatterns")
  void testStripJdbcEscapes(String input, String expected, String errorMessage) {
    assertEquals(expected, WildcardUtil.stripJdbcEscapes(input), errorMessage);
  }

  private static Stream<Arguments> isJdbcPatternCases() {
    return Stream.of(
        Arguments.of(null, false, "null is not a pattern"),
        Arguments.of("", false, "empty string is not a pattern"),
        Arguments.of("simple", false, "plain literal is not a pattern"),
        Arguments.of("%", true, "bare % is a pattern"),
        Arguments.of("_", true, "bare _ is a pattern"),
        Arguments.of("cat%", true, "trailing % is a pattern"),
        Arguments.of("%log%", true, "leading and trailing % is a pattern"),
        Arguments.of("my_cat", true, "underscore is a pattern"),
        Arguments.of("\\%", false, "escaped % is NOT a pattern"),
        Arguments.of("\\_", false, "escaped _ is NOT a pattern"),
        Arguments.of("cat\\_main", false, "escaped underscore in literal is not a pattern"),
        Arguments.of("cat\\_main%", true, "escaped underscore but bare % makes it a pattern"));
  }

  @ParameterizedTest
  @MethodSource("isJdbcPatternCases")
  void testIsJdbcPattern(String input, boolean expected, String message) {
    assertEquals(expected, WildcardUtil.isJdbcPattern(input), message);
  }

  private static Stream<Arguments> isMatchAllCatalogPatternCases() {
    return Stream.of(
        Arguments.of(null, true, "null matches all"),
        Arguments.of("%", true, "% matches all"),
        Arguments.of("", false, "empty string does not match all"),
        Arguments.of("main", false, "literal catalog does not match all"),
        Arguments.of("main%", false, "partial pattern does not match all"),
        Arguments.of("%main%", false, "partial pattern does not match all"));
  }

  @ParameterizedTest
  @MethodSource("isMatchAllCatalogPatternCases")
  void testIsMatchAllCatalogPattern(String input, boolean expected, String message) {
    assertEquals(expected, WildcardUtil.isMatchAllCatalogPattern(input), message);
  }
}
