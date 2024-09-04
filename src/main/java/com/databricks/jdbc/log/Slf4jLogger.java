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

  /** {@inheritDoc} */
  @Override
  public void debug(String message) {
    logger.debug(message);
  }

  /** {@inheritDoc} */
  @Override
  public void info(String message) {
    logger.info(message);
  }

  /** {@inheritDoc} */
  @Override
  public void warn(String message) {
    logger.warn(message);
  }

  /** {@inheritDoc} */
  @Override
  public void error(String message) {
    logger.error(message);
  }

  /** {@inheritDoc} */
  @Override
  public void error(String message, Throwable throwable) {
    logger.error(message, throwable);
  }
}
