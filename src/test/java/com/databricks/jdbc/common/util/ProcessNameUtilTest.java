package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ProcessNameUtilTest {

  @ParameterizedTest
  @MethodSource("processNameFormats")
  void testGetProcessName(String command, String expected) {
    if (command != null) {
      System.setProperty("sun.java.command", command);
    } else {
      System.clearProperty("sun.java.command");
    }

    try {
      String processName = ProcessNameUtil.getProcessName();
      assertNotNull(processName);
      if (expected != null) {
        assertEquals(expected, processName);
      }
    } finally {
      System.clearProperty("sun.java.command");
    }
  }

  @ParameterizedTest
  @MethodSource("processHandlePaths")
  void testProcessHandlePaths(String processPath, String expectedName, String description) {
    // On JDK 8 build, ProcessHandle API is not available; implementation
    // relies on sun.java.command fallback. Simulate via property only.
    System.setProperty("sun.java.command", processPath);
    try {
      String processName = ProcessNameUtil.getProcessName();
      assertEquals(expectedName, processName, description);
    } finally {
      System.clearProperty("sun.java.command");
    }
  }

  static Stream<Arguments> processHandlePaths() {
    return Stream.of(
        Arguments.of(
            "/Applications/DBeaver.app/Contents/MacOS/dbeaver",
            "dbeaver",
            "Should extract 'dbeaver' from Mac path"),
        Arguments.of(
            "C:\\Program Files\\DBeaver\\dbeaver.exe",
            "dbeaver",
            "Should extract 'dbeaver' from Windows path"),
        Arguments.of("/usr/bin/java", "java", "Should extract 'java' from Unix path"),
        Arguments.of(
            "C:\\Program Files\\Java\\bin\\java.exe",
            "java",
            "Should extract 'java' from Windows Java path"));
  }

  static Object[][] processNameFormats() {
    return new Object[][] {
      {"com.example.MyApp", "MyApp"},
      {"com.example.MyApp arg1", "MyApp"},
      {"MyApp", "MyApp"},
      {null, null}, // For null case, we just verify we get a non-null result
    };
  }
}
