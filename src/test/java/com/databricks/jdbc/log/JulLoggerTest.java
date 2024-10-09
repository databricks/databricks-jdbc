package com.databricks.jdbc.log;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class JulLoggerTest {

  private JulLogger julLogger;

  private Logger mockLogger;

  @BeforeEach
  void setUp() {
    mockLogger = Mockito.mock(Logger.class);
    julLogger = new JulLogger("test");
    julLogger.logger = mockLogger;
  }

  @AfterEach
  void tearDown() {
    // Reset the logger after each test
    JulLogger.isLoggerInitialized = false;

    Logger logger = Logger.getLogger(JulLogger.PARENT_CLASS_PREFIX);
    logger.setLevel(null);
    for (Handler handler : logger.getHandlers()) {
      logger.removeHandler(handler);
    }
    logger.setUseParentHandlers(true);
  }

  @Test
  void testTrace() {
    julLogger.trace("Test trace message");
    verify(mockLogger)
        .logp(
            Level.FINEST,
            "com.databricks.jdbc.log.JulLoggerTest",
            "testTrace",
            "Test trace message");
  }

  @Test
  void testDebug() {
    julLogger.debug("Test debug message");
    verify(mockLogger)
        .logp(
            Level.FINE, "com.databricks.jdbc.log.JulLoggerTest", "testDebug", "Test debug message");
  }

  @Test
  void testInfo() {
    julLogger.info("Test info message");
    verify(mockLogger)
        .logp(Level.INFO, "com.databricks.jdbc.log.JulLoggerTest", "testInfo", "Test info message");
  }

  @Test
  void testWarn() {
    julLogger.warn("Test warn message");
    verify(mockLogger)
        .logp(
            Level.WARNING,
            "com.databricks.jdbc.log.JulLoggerTest",
            "testWarn",
            "Test warn message");
  }

  @Test
  void testError() {
    julLogger.error("Test error message");
    verify(mockLogger)
        .logp(
            Level.SEVERE,
            "com.databricks.jdbc.log.JulLoggerTest",
            "testError",
            "Test error message");
  }

  @Test
  void testErrorWithThrowable() {
    Exception exception = new Exception("Test exception");
    julLogger.error(exception, "Test error message");
    verify(mockLogger)
        .logp(
            Level.SEVERE,
            "com.databricks.jdbc.log.JulLoggerTest",
            "testErrorWithThrowable",
            "Test error message",
            exception);
  }

  @Test
  void testInitLoggerWithStdout() throws IOException {
    JulLogger.initLogger(Level.INFO, JulLogger.STDOUT, 1024, 1);
    Logger jdbcLogger = Logger.getLogger(JulLogger.PARENT_CLASS_PREFIX);
    assertEquals(Level.INFO, jdbcLogger.getLevel());
    assertInstanceOf(StreamHandler.class, jdbcLogger.getHandlers()[0]);
  }

  @Test
  void testInitLoggerWithFileHandler(@TempDir Path tempDir) throws IOException {
    String logDir = tempDir.toString();
    JulLogger.initLogger(Level.INFO, logDir, 1024, 1);
    Logger jdbcLogger = Logger.getLogger(JulLogger.PARENT_CLASS_PREFIX);
    assertEquals(Level.INFO, jdbcLogger.getLevel());
    assertInstanceOf(FileHandler.class, jdbcLogger.getHandlers()[0]);
    assertTrue(Files.exists(tempDir.resolve(JulLogger.DATABRICKS_LOG_FILE)));
  }

  @Test
  void testGetCaller() {
    String[] caller = simulateLoggingCall();
    assertEquals(JulLoggerTest.class.getName(), caller[0]);
    assertEquals("methodCallingLogger", caller[1]);
  }

  @Test
  void testGetLogPatternStdout() throws IOException {
    assertEquals(JulLogger.STDOUT, JulLogger.getLogPattern(JulLogger.STDOUT));
  }

  @Test
  void testGetLogPatternWithDirectory(@TempDir Path tempDir) throws IOException {
    String logDir = tempDir.toString();
    String expected = tempDir.resolve(JulLogger.DATABRICKS_LOG_FILE).toString();
    assertEquals(expected, JulLogger.getLogPattern(logDir));
    assertTrue(Files.exists(tempDir));
  }

  @Test
  void testGetLogPatternWhenDirectoryCannotBeCreated() {
    // Create a mock Path
    Path mockPath = Mockito.mock(Path.class);
    Mockito.when(mockPath.toString()).thenReturn("/non/existent/directory");
    Mockito.when(mockPath.resolve(Mockito.anyString())).thenReturn(mockPath);

    // Mock the static Paths.get method to return our mock Path
    try (MockedStatic<Paths> mockedPaths = Mockito.mockStatic(Paths.class)) {
      mockedPaths.when(() -> Paths.get(Mockito.anyString())).thenReturn(mockPath);

      // Mock the static Files.notExists to return true
      // Mock the static Files.createDirectories method to throw an exception when trying to create
      // directories
      try (MockedStatic<Files> mockedFiles = Mockito.mockStatic(Files.class)) {
        mockedFiles.when(() -> Files.notExists(Mockito.any(Path.class))).thenReturn(true);
        mockedFiles
            .when(() -> Files.createDirectories(Mockito.any(Path.class)))
            .thenThrow(new IOException("Directory creation failed"));

        // Call the method and assert the result is STDOUT
        String result = JulLogger.getLogPattern("/non/existent/directory");
        assertEquals(JulLogger.STDOUT, result);
      }
    }
  }

  private String[] simulateLoggingCall() {
    return methodCallingLogger();
  }

  private String[] methodCallingLogger() {
    // Simulate a logging call with a log method
    return info();
  }

  private String[] info() {
    return JulLogger.getCaller();
  }
}
