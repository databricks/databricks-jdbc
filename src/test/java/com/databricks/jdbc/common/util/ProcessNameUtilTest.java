package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

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
    System.clearProperty("sun.java.command");
    try (MockedStatic<ProcessHandle> processHandleMock = mockStatic(ProcessHandle.class)) {
      ProcessHandle mockHandle = org.mockito.Mockito.mock(ProcessHandle.class);
      ProcessHandle.Info mockInfo = org.mockito.Mockito.mock(ProcessHandle.Info.class);

      when(ProcessHandle.current()).thenReturn(mockHandle);
      when(mockHandle.info()).thenReturn(mockInfo);
      when(mockInfo.command()).thenReturn(Optional.of(processPath));

      String processName = ProcessNameUtil.getProcessName();
      assertEquals(expectedName, processName, description);
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
