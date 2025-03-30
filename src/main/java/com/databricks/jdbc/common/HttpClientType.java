package com.databricks.jdbc.common;

/**
 * Enumerates the types of HTTP clients supported by the Databricks JDBC driver.
 *
 * <p>This enum defines the available HTTP client implementations that can be used for making HTTP
 * requests to Databricks services.
 */
public enum HttpClientType {
  /** Standard HTTP client implementation for general-purpose requests. */
  COMMON,

  /** Specialized HTTP client implementation optimized for UC volume operations. */
  VOLUME;
}
