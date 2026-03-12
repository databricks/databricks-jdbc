package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
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

  static Object[][] processNameFormats() {
    return new Object[][] {
      {"com.example.MyApp", "MyApp"},
      {"com.example.MyApp arg1", "MyApp"},
      {"MyApp", "MyApp"},
      {null, null}, // For null case, we just verify we get a non-null result
    };
  }
}
