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

  static Stream<Arguments> serverPatterns() {
    return Stream.of(
        Arguments.of(null, null, null),
        Arguments.of("", "", null),
        Arguments.of("plain%_", "plain%_", null),
        Arguments.of("q'uote", "q_uote", "q'uote"),
        Arguments.of("x'q_ji_1598", "x_q_ji_1598", "x'q_ji_1598"),
        Arguments.of("x'q\\_ji\\_1598", "x_q\\_ji\\_1598", "x'q_ji_1598"),
        Arguments.of("a\\b", "ab", "ab"),
        Arguments.of("a\\\\b", "a_b", "a\\b"),
        Arguments.of("a\\\\\\\\b", "a__b", "a\\\\b"),
        Arguments.of("a\\_b", "a\\_b", "a_b"),
        Arguments.of("a\\%b", "a\\%b", "a%b"),
        Arguments.of("a\\\\\\_b", "a_\\_b", "a\\_b"),
        Arguments.of("a\\\\_b", "a__b", "a\\xb"),
        Arguments.of("a\\\\%b", "a_%b", "a\\xyzb"),
        Arguments.of("a\\\\'b", "a__b", "a\\'b"),
        Arguments.of("a\\\\[b]", "a__b_", "a\\[b]"),
        Arguments.of("a\\", "a", "a"),
        Arguments.of("a\\.", "a_", "a."),
        Arguments.of("t😀\\_a", "t😀\\_a", "t😀_a"),
        Arguments.of("𠮷野家\\_sales", "𠮷野家\\_sales", "𠮷野家_sales"),
        Arguments.of("t\\😀\\_a", "t😀\\_a", "t😀_a"),
        Arguments.of("t\\𠮷\\_a", "t𠮷\\_a", "t𠮷_a"));
  }

  @ParameterizedTest
  @MethodSource("serverPatterns")
  void normalizesServerPatternsAndCompilesJdbcMatches(
      String pattern, String expectedServerPattern, String matchingName) {
    assertEquals(expectedServerPattern, WildcardUtil.jdbcPatternToServer(pattern));
    java.util.regex.Pattern matcher = WildcardUtil.compileJdbcPattern(pattern);
    if (matchingName == null) {
      assertNull(matcher);
      assertSame(pattern, WildcardUtil.jdbcPatternToServer(pattern));
    } else {
      assertTrue(matcher.matcher(matchingName).matches());
      assertTrue(matcher.matcher(matchingName.toUpperCase(java.util.Locale.ROOT)).matches());
      assertFalse(matcher.matcher("not-" + matchingName).matches());
    }
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
}
