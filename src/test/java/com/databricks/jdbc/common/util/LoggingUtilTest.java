package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.common.LogLevel;
import java.util.logging.Level;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class LoggingUtilTest {
  @Test
  void testSetupLogger() {
    assertDoesNotThrow(() -> LoggingUtil.setupLogger("test", 1, 1, LogLevel.DEBUG));
    assertDoesNotThrow(() -> LoggingUtil.setupLogger("test.log", 1, 1, LogLevel.DEBUG));
  }

  @Test
  void testSetupLoggerWithOffLevel() {
    // When log level is OFF, setupLogger initializes logger with Level.OFF to suppress all output.
    // It uses STDOUT to avoid file system access issues in restricted environments.
    // This should not throw an exception even if the log path is not writable.
    assertDoesNotThrow(() -> LoggingUtil.setupLogger("/", 1, 1, LogLevel.OFF));
    assertDoesNotThrow(() -> LoggingUtil.setupLogger("/invalid/path", 1, 1, LogLevel.OFF));
  }

  @ParameterizedTest
  @MethodSource("logLevelToJulLevelProvider")
  void testToJulLevel(LogLevel input, Level expected) {
    LoggingUtil loggingUtil = new LoggingUtil(); // test constructor
    assertEquals(expected, loggingUtil.toJulLevel(input));
  }

  static Stream<Arguments> logLevelToJulLevelProvider() {
    return Stream.of(
        org.junit.jupiter.params.provider.Arguments.of(LogLevel.DEBUG, Level.FINE),
        org.junit.jupiter.params.provider.Arguments.of(LogLevel.ERROR, Level.SEVERE),
        org.junit.jupiter.params.provider.Arguments.of(LogLevel.FATAL, Level.SEVERE),
        org.junit.jupiter.params.provider.Arguments.of(LogLevel.INFO, Level.INFO),
        org.junit.jupiter.params.provider.Arguments.of(LogLevel.TRACE, Level.FINEST),
        org.junit.jupiter.params.provider.Arguments.of(LogLevel.WARN, Level.WARNING),
        org.junit.jupiter.params.provider.Arguments.of(LogLevel.OFF, Level.OFF));
  }
}
