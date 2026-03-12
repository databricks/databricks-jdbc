package com.databricks.jdbc.log;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JdbcLoggerFactoryTest {

  @BeforeEach
  void setUp() {
    // Clear any system properties set in previous tests
    System.clearProperty(JdbcLoggerFactory.LOGGER_IMPL_PROPERTY);
    // Reset the loggerImpl field
    setLoggerImplToNull();
  }

  @AfterEach
  void tearDown() {
    System.clearProperty(JdbcLoggerFactory.LOGGER_IMPL_PROPERTY);
    setLoggerImplToNull();
  }

  @Test
  void testGetLoggerWithClass_DefaultToJul() {
    JdbcLogger logger = JdbcLoggerFactory.getLogger(JdbcLoggerFactoryTest.class);
    assertInstanceOf(JulLogger.class, logger);
  }

  @Test
  void testGetLoggerWithString_DefaultToJul() {
    JdbcLogger logger = JdbcLoggerFactory.getLogger("TestLogger");
    assertInstanceOf(JulLogger.class, logger);
  }

  @Test
  void testGetLoggerWithClass_JdkLogger() {
    System.setProperty(JdbcLoggerFactory.LOGGER_IMPL_PROPERTY, "JDKLOGGER");
    JdbcLogger logger = JdbcLoggerFactory.getLogger(JdbcLoggerFactoryTest.class);
    assertInstanceOf(JulLogger.class, logger);
  }

  @Test
  void testGetLoggerWithString_JdkLogger() {
    System.setProperty(JdbcLoggerFactory.LOGGER_IMPL_PROPERTY, "JDKLOGGER");
    JdbcLogger logger = JdbcLoggerFactory.getLogger("TestLogger");
    assertInstanceOf(JulLogger.class, logger);
  }

  @Test
  void testGetLoggerWithClass_Slf4jLogger() {
    System.setProperty(JdbcLoggerFactory.LOGGER_IMPL_PROPERTY, "SLF4JLOGGER");
    JdbcLogger logger = JdbcLoggerFactory.getLogger(JdbcLoggerFactoryTest.class);
    assertInstanceOf(Slf4jLogger.class, logger);
  }

  @Test
  void testGetLoggerWithString_Slf4jLogger() {
    System.setProperty(JdbcLoggerFactory.LOGGER_IMPL_PROPERTY, "SLF4JLOGGER");
    JdbcLogger logger = JdbcLoggerFactory.getLogger("TestLogger");
    assertInstanceOf(Slf4jLogger.class, logger);
  }

  @Test
  void testGetLogger_InvalidProperty() {
    System.setProperty(JdbcLoggerFactory.LOGGER_IMPL_PROPERTY, "INVALID_LOGGER");
    JdbcLogger logger = JdbcLoggerFactory.getLogger("TestLogger");
    // Default to JUL logger
    assertInstanceOf(JulLogger.class, logger);
  }

  @Test
  void testLoggerImpl_FromString() {
    assertTrue(JdbcLoggerFactory.LoggerImpl.fromString("SLF4JLOGGER").isPresent());
    assertTrue(JdbcLoggerFactory.LoggerImpl.fromString("JDKLOGGER").isPresent());
    assertFalse(JdbcLoggerFactory.LoggerImpl.fromString("INVALID_LOGGER").isPresent());
    assertFalse(JdbcLoggerFactory.LoggerImpl.fromString(null).isPresent());
  }

  @Test
  void testLoggerImpl_CaseInsensitive() {
    Optional<JdbcLoggerFactory.LoggerImpl> slf4jLogger =
        JdbcLoggerFactory.LoggerImpl.fromString("slf4jlogger");
    Optional<JdbcLoggerFactory.LoggerImpl> jdkLogger =
        JdbcLoggerFactory.LoggerImpl.fromString("jdklogger");

    assertTrue(slf4jLogger.isPresent(), "SLF4J logger should be present");
    assertTrue(jdkLogger.isPresent(), "JDK logger should be present");

    assertEquals(JdbcLoggerFactory.LoggerImpl.SLF4JLOGGER, slf4jLogger.get());
    assertEquals(JdbcLoggerFactory.LoggerImpl.JDKLOGGER, jdkLogger.get());
  }

  /** Helper method to reset the loggerImpl field using reflection. */
  private void setLoggerImplToNull() {
    try {
      java.lang.reflect.Field field = JdbcLoggerFactory.class.getDeclaredField("loggerImpl");
      field.setAccessible(true);
      field.set(null, null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Failed to reset loggerImpl", e);
    }
  }
}
