package com.databricks.jdbc.auth;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.oauth.Token;
import com.databricks.sdk.core.oauth.TokenCache;

/**
 * A no-operation implementation of TokenCache that does nothing. Used when token caching is
 * explicitly disabled.
 */
public class NoOpTokenCache implements TokenCache {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(NoOpTokenCache.class);

  @Override
  public void save(Token token) {
    LOGGER.debug("Token caching is disabled, skipping save operation");
  }

  @Override
  public Token load() {
    LOGGER.debug("Token caching is disabled, skipping load operation");
    return null;
  }
}
