package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.databricks.jdbc.common.LogLevel;
import org.junit.jupiter.api.Test;

public class LoggingUtilTest {
  @Test
  void testSetupLogger() {
    assertDoesNotThrow(() -> LoggingUtil.setupLogger("test", 1, 1, LogLevel.DEBUG));
    assertDoesNotThrow(() -> LoggingUtil.setupLogger("test.log", 1, 1, LogLevel.DEBUG));
  }
}
