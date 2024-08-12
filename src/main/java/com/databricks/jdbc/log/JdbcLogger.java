package com.databricks.jdbc.log;

/**
 * The interface defines logging methods for various levels of importance. Implementations of this
 * interface can be used to integrate with different logging frameworks.
 */
public interface JdbcLogger {
  void trace(String message);

  void debug(String message);

  void info(String message);

  void warn(String message);

  void error(String message);

  void error(String message, Throwable throwable);
}
