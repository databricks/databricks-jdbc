package com.databricks.jdbc.auth;

import com.databricks.sdk.core.oauth.Token;
import com.databricks.sdk.core.oauth.TokenCache;
import java.time.Instant;

/**
 * Utility class for token caching operations shared across credential providers.
 *
 * <p>Provides common functionality for working with token caches, including expiration validation
 * with safety buffers to prevent using tokens close to expiration.
 *
 * <p>This utility follows SDK patterns while adding the safety buffer recommended in ES-1627504
 * incident analysis to prevent token expiration during request processing.
 */
public class TokenCacheUtils {

  /**
   * Safety buffer in seconds to prevent using tokens close to expiration. This buffer accounts for:
   *
   * <ul>
   *   <li>Clock skew between client and server
   *   <li>Network latency for token refresh operations
   *   <li>Processing time for the request
   * </ul>
   *
   * <p>Set to 5 minutes (300 seconds) as recommended in ES-1627504 incident analysis.
   */
  public static final long EXPIRATION_BUFFER_SECONDS = 300;

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
   * Checks if a token has expired or is within the expiration buffer window.
   *
   * <p>This method implements a conservative expiration check that considers tokens as expired if
   * they are within {@link #EXPIRATION_BUFFER_SECONDS} of their expiration time. This prevents edge
   * cases where a token expires during request processing.
   *
   * @param token The token to check for expiration. If null, returns true.
   * @return true if the token is null or is expired/expiring soon; false if the token is still
   *     valid
   */
  public static boolean isExpired(Token token) {
    if (token == null) {
      return true;
    }

    Instant expiry = token.getExpiry();
    if (expiry == null) {
      // Token has no expiration, consider it expired for safety
      return true;
    }

    // Check if token is expired or within the safety buffer
    Instant expirationThreshold = Instant.now().plusSeconds(EXPIRATION_BUFFER_SECONDS);
    return expiry.isBefore(expirationThreshold);
  }

  /**
   * Attempts to load a valid token from the cache.
   *
   * <p>This method combines cache loading with expiration validation in a single operation. If the
   * cached token exists and is still valid (not expired or expiring soon), it is returned.
   * Otherwise, null is returned to indicate that a fresh token should be obtained.
   *
   * <p>This is the recommended way to use TokenCache in credential providers as it ensures tokens
   * are always validated before use.
   *
   * @param cache The token cache to load from. If null, returns null.
   * @return A valid token if one exists in the cache, or null if no valid cached token is available
   */
  public static Token loadValidToken(TokenCache cache) {
    if (cache == null) {
      return null;
    }

    Token cachedToken = cache.load();
    if (cachedToken != null && !isExpired(cachedToken)) {
      return cachedToken;
    }

    return null;
  }
}
