package com.databricks.jdbc.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code Slf4jLogger} class implements the {@code JdbcLogger} interface, providing an
 * SLF4J-based logging solution.
 */
public class Slf4jLogger implements JdbcLogger {

  protected Logger logger;

  /** Constructs a new {@code Slf4jLogger} object with the specified class. */
  public Slf4jLogger(Class<?> clazz) {
    this.logger = LoggerFactory.getLogger(clazz);
  }

  /** Constructs a new {@code Slf4jLogger} object with the specified name. */
  public Slf4jLogger(String name) {
    this.logger = LoggerFactory.getLogger(name);
  }

  /** {@inheritDoc} */
  @Override
  public void trace(String message) {
    logger.trace(message);
  }

  @Override
  public void trace(String format, Object... arguments) {
    logger.trace(format, arguments);
  }

  /** {@inheritDoc} */
  @Override
  public void debug(String message) {
    logger.debug(message);
  }

  @Override
  public void debug(String format, Object... arguments) {
    logger.debug(format, arguments);
  }

  /** {@inheritDoc} */
  @Override
  public void info(String message) {
    logger.info(message);
  }

  @Override
  public void info(String format, Object... arguments) {
    logger.info(format, arguments);
  }

  /** {@inheritDoc} */
  @Override
  public void warn(String message) {
    logger.warn(message);
  }

  @Override
  public void warn(String format, Object... arguments) {
    logger.warn(format, arguments);
  }

  /** {@inheritDoc} */
  @Override
  public void error(String message) {
    logger.error(message);
  }

  @Override
  public void error(String format, Object... arguments) {
    logger.error(format, arguments);
  }

  /** {@inheritDoc} */
  @Override
  public void error(Throwable throwable, String message) {
    logger.error(message, throwable);
  }

  @Override
  public void error(Throwable throwable, String format, Object... arguments) {
    logger.error(format, throwable, arguments);
  }
}
