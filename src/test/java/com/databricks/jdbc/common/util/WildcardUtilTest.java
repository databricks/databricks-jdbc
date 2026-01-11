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
}
