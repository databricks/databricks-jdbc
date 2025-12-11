package com.databricks.jdbc.auth;

import com.databricks.sdk.core.oauth.Token;
import com.databricks.sdk.core.oauth.TokenCache;

/**
 * Utility class for token caching operations shared across credential providers.
 *
 * <p>Provides common functionality for working with token caches following SDK patterns.
 */
public class TokenCacheUtils {

  /**
   * Default passphrase suffix used when generating encryption passphrases for token caches. This is
   * combined with host and clientId to create unique passphrases.
   */
  private static final String DEFAULT_PASSPHRASE_SUFFIX = "databricks-jdbc-token-cache";

  /**
   * Generates a default passphrase for token cache encryption. Combines host, clientId, and a
   * standard suffix to create a unique passphrase.
   *
   * @param host The Databricks host URL
   * @param clientId The OAuth client ID (can be null)
   * @return A generated passphrase string
   */
  public static String getDefaultPassphrase(String host, String clientId) {
    return (host != null ? host : "")
        + (clientId != null ? clientId : "")
        + DEFAULT_PASSPHRASE_SUFFIX;
  }

  /**
   * Creates an EncryptedFileTokenCache with the given parameters. Uses the configured passphrase if
   * available, otherwise generates a default passphrase.
   *
   * @param cachePath The path where the token cache file will be stored
   * @param configuredPassphrase The passphrase from configuration (can be null)
   * @param host The Databricks host URL (used for default passphrase generation)
   * @param clientId The OAuth client ID (used for default passphrase generation, can be null)
   * @return An EncryptedFileTokenCache instance
   */
  public static TokenCache createEncryptedCache(
      java.nio.file.Path cachePath, String configuredPassphrase, String host, String clientId) {
    String passphrase = configuredPassphrase;
    if (passphrase == null || passphrase.isEmpty()) {
      passphrase = getDefaultPassphrase(host, clientId);
    }
    return new EncryptedFileTokenCache(cachePath, passphrase);
  }

  /**
   * Attempts to load a token from the cache.
   *
   * <p>This is a convenience method that safely handles null caches. Delegates token expiration
   * checking to the underlying authentication provider, following SDK patterns.
   *
   * @param cache The token cache to load from. If null, returns null.
   * @return The cached token if one exists, or null if no cached token is available
   */
  public static Token loadValidToken(TokenCache cache) {
    if (cache == null) {
      return null;
    }
    return cache.load();
  }
}
