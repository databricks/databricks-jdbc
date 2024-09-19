package com.databricks.jdbc.log;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory class for creating JDBC logger instances. This class supports creating loggers based on
 * the specified logger implementation. The logger implementation can be specified through a system
 * property {@value #LOGGER_IMPL_PROPERTY}. Supported logger implementations include SLF4J and JDK
 * logging.
 */
public class JdbcLoggerFactory {

  public static final String LOGGER_IMPL_PROPERTY = "com.databricks.jdbc.loggerImpl";

  /** Holds the singleton instance of the logger implementation determined at runtime. */
  private static volatile LoggerImpl loggerImpl;

  /** A map of logger implementation names to their corresponding {@link LoggerImpl} enum values. */
  private static final Map<String, LoggerImpl> LOGGER_IMPL_MAP = new ConcurrentHashMap<>();

  static {
    for (LoggerImpl impl : LoggerImpl.values()) {
      LOGGER_IMPL_MAP.put(impl.getLoggerImplClassName().toLowerCase(), impl);
    }
  }

  /** Private constructor to prevent instantiation. */
  private JdbcLoggerFactory() {
    throw new AssertionError("JdbcLoggerFactory is not instantiable.");
  }

  /** Enum representing the supported logger implementations. */
  enum LoggerImpl {
    SLF4JLOGGER("SLF4JLOGGER"),
    JDKLOGGER("JDKLOGGER");

    private final String loggerImplClassName;

    LoggerImpl(String loggerClass) {
      this.loggerImplClassName = loggerClass;
    }

    public String getLoggerImplClassName() {
      return loggerImplClassName;
    }

    /**
     * Returns an {@link Optional} containing the {@link LoggerImpl} corresponding to the given
     * logger implementation class name, if it exists.
     */
    public static Optional<LoggerImpl> fromString(String loggerImplClassName) {
      return Optional.ofNullable(loggerImplClassName)
          .map(String::toLowerCase)
          .map(LOGGER_IMPL_MAP::get);
    }
  }

  /** Returns a {@link JdbcLogger} instance for the specified class. */
  public static JdbcLogger getLogger(Class<?> clazz) {
    resolveLoggerImpl();

    switch (loggerImpl) {
      case JDKLOGGER:
        return new JulLogger(clazz.getName());
      case SLF4JLOGGER:
      default:
        return new Slf4jLogger(clazz);
    }
  }

  /** Returns a {@link JdbcLogger} instance for the specified name. */
  public static JdbcLogger getLogger(String name) {
    resolveLoggerImpl();

    switch (loggerImpl) {
      case JDKLOGGER:
        return new JulLogger(name);
      case SLF4JLOGGER:
      default:
        return new Slf4jLogger(name);
    }
  }

  /**
   * Resolves the logger implementation to be used. This method checks the system property {@value
   * #LOGGER_IMPL_PROPERTY} to determine the logger implementation. If the property is not set, it
   * defaults to {@link LoggerImpl#SLF4JLOGGER}.
   */
  private static void resolveLoggerImpl() {
    if (loggerImpl == null) {
      synchronized (JdbcLoggerFactory.class) {
        if (loggerImpl == null) {
          String logger = System.getProperty(LOGGER_IMPL_PROPERTY);
          loggerImpl = LoggerImpl.fromString(logger).orElse(LoggerImpl.JDKLOGGER);
        }
      }
    }
  }
}
