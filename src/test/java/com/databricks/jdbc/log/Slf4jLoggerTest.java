package com.databricks.jdbc.log;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class Slf4jLoggerTest {
  private Logger mockLogger;

  private Slf4jLogger slf4jLogger;

  @BeforeEach
  public void setUp() {
    mockLogger = Mockito.mock(Logger.class);
    slf4jLogger =
        new Slf4jLogger("test") {
          {
            this.logger = mockLogger; // Inject the mock logger
          }
        };
  }

  @Test
  public void testTrace() {
    String message = "trace message";
    slf4jLogger.trace(message);
    verify(mockLogger, times(1)).trace(message);
  }

  @Test
  public void testDebug() {
    String message = "debug message";
    slf4jLogger.debug(message);
    verify(mockLogger, times(1)).debug(message);
  }

  @Test
  public void testInfo() {
    String message = "info message";
    slf4jLogger.info(message);
    verify(mockLogger, times(1)).info(message);
  }

  @Test
  public void testWarn() {
    String message = "warn message";
    slf4jLogger.warn(message);
    verify(mockLogger, times(1)).warn(message);
  }

  @Test
  public void testError() {
    String message = "error message";
    slf4jLogger.error(message);
    verify(mockLogger, times(1)).error(message);
  }

  @Test
  public void testErrorWithThrowable() {
    String message = "error message";
    Throwable throwable = new RuntimeException("error");
    slf4jLogger.error(throwable, message);
    verify(mockLogger, times(1)).error(message, throwable);
  }

  @Test
  public void testConstructorWithClass() throws NoSuchFieldException, IllegalAccessException {
    Class<?> clazz = Slf4jLogger.class;
    Slf4jLogger loggerInstance = new Slf4jLogger(clazz);

    Field loggerField = Slf4jLogger.class.getDeclaredField("logger");
    loggerField.setAccessible(true);

    Logger logger = (Logger) loggerField.get(loggerInstance);

    assertNotNull(logger);
    assertEquals(logger.getName(), clazz.getName());
  }
}
