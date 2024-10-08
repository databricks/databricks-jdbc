package com.databricks.jdbc.log;

/**
 * The interface defines logging methods for various levels of importance. Implementations of this
 * interface can be used to integrate with different logging frameworks.
 */
public interface JdbcLogger {
  void trace(String message);

  void trace(String format, Object... arguments);

  void debug(String message);

  void debug(String format, Object... arguments);

  void info(String message);

  void info(String format, Object... arguments);

  void warn(String message);

  void warn(String format, Object... arguments);

  void error(String message);

  void error(String format, Object... arguments);

  void error(Throwable throwable, String message);

  void error(Throwable throwable, String format, Object... arguments);
}
