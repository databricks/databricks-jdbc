package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.core.oauth.Token;
import com.databricks.sdk.core.oauth.TokenCache;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

public class TokenCacheUtilsTest {

  private static final String TEST_HOST = "https://test.databricks.com";
  private static final String TEST_CLIENT_ID = "test-client-id";
  private static final String ACCESS_TOKEN = "test-access-token";
  private static final String TOKEN_TYPE = "Bearer";
  private static final String REFRESH_TOKEN = "test-refresh-token";

  @TempDir Path tempDir;

  // ============================================
  // Tests for loadValidToken()
  // ============================================

  @Test
  void testLoadValidToken_NullCache() {
    Token result = TokenCacheUtils.loadValidToken(null);
    assertNull(result, "Loading from null cache should return null");
  }

  @Test
  void testLoadValidToken_CacheReturnsNull() {
    TokenCache mockCache = Mockito.mock(TokenCache.class);
    Mockito.when(mockCache.load()).thenReturn(null);

    Token result = TokenCacheUtils.loadValidToken(mockCache);
    assertNull(result, "Loading null token from cache should return null");
  }

  @Test
  void testLoadValidToken_CacheReturnsToken() {
    TokenCache mockCache = Mockito.mock(TokenCache.class);
    Instant expiry = Instant.now().plus(1, ChronoUnit.HOURS);
    Token cachedToken = new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, expiry);
    Mockito.when(mockCache.load()).thenReturn(cachedToken);

    Token result = TokenCacheUtils.loadValidToken(mockCache);
    assertNotNull(result, "Loading token from cache should return the token");
    assertEquals(cachedToken, result, "Returned token should match the cached token");
  }

  // ============================================
  // Tests for getDefaultPassphrase()
  // ============================================

  @Test
  void testGetDefaultPassphrase_AllParametersProvided() {
    String passphrase = TokenCacheUtils.getDefaultPassphrase(TEST_HOST, TEST_CLIENT_ID);

    assertNotNull(passphrase, "Passphrase should not be null");
    assertTrue(passphrase.contains(TEST_HOST), "Passphrase should contain host");
    assertTrue(passphrase.contains(TEST_CLIENT_ID), "Passphrase should contain client ID");
    assertTrue(
        passphrase.endsWith("databricks-jdbc-token-cache"),
        "Passphrase should end with default suffix");
  }

  @Test
  void testGetDefaultPassphrase_NullHost() {
    String passphrase = TokenCacheUtils.getDefaultPassphrase(null, TEST_CLIENT_ID);

    assertNotNull(passphrase, "Passphrase should not be null");
    assertTrue(passphrase.contains(TEST_CLIENT_ID), "Passphrase should contain client ID");
    assertTrue(
        passphrase.endsWith("databricks-jdbc-token-cache"),
        "Passphrase should end with default suffix");
  }

  @Test
  void testGetDefaultPassphrase_NullClientId() {
    String passphrase = TokenCacheUtils.getDefaultPassphrase(TEST_HOST, null);

    assertNotNull(passphrase, "Passphrase should not be null");
    assertTrue(passphrase.contains(TEST_HOST), "Passphrase should contain host");
    assertTrue(
        passphrase.endsWith("databricks-jdbc-token-cache"),
        "Passphrase should end with default suffix");
  }

  @Test
  void testGetDefaultPassphrase_BothParametersNull() {
    String passphrase = TokenCacheUtils.getDefaultPassphrase(null, null);

    assertNotNull(passphrase, "Passphrase should not be null");
    assertEquals(
        "databricks-jdbc-token-cache",
        passphrase,
        "Passphrase should be just the default suffix when both parameters are null");
  }

  @Test
  void testGetDefaultPassphrase_DifferentHostsShouldProduceDifferentPassphrases() {
    String passphrase1 =
        TokenCacheUtils.getDefaultPassphrase("https://host1.databricks.com", TEST_CLIENT_ID);
    String passphrase2 =
        TokenCacheUtils.getDefaultPassphrase("https://host2.databricks.com", TEST_CLIENT_ID);

    assertNotEquals(
        passphrase1, passphrase2, "Different hosts should produce different passphrases");
  }

  @Test
  void testGetDefaultPassphrase_DifferentClientIdsShouldProduceDifferentPassphrases() {
    String passphrase1 = TokenCacheUtils.getDefaultPassphrase(TEST_HOST, "client-id-1");
    String passphrase2 = TokenCacheUtils.getDefaultPassphrase(TEST_HOST, "client-id-2");

    assertNotEquals(
        passphrase1, passphrase2, "Different client IDs should produce different passphrases");
  }

  // ============================================
  // Tests for createEncryptedCache()
  // ============================================

  @Test
  void testCreateEncryptedCache_WithConfiguredPassphrase() {
    Path cachePath = tempDir.resolve("token-cache");
    String configuredPassphrase = "my-custom-passphrase";

    TokenCache cache =
        TokenCacheUtils.createEncryptedCache(
            cachePath, configuredPassphrase, TEST_HOST, TEST_CLIENT_ID);

    assertNotNull(cache, "Cache should not be null");
    assertInstanceOf(
        EncryptedFileTokenCache.class, cache, "Cache should be an EncryptedFileTokenCache");
  }

  @Test
  void testCreateEncryptedCache_WithNullPassphrase() {
    Path cachePath = tempDir.resolve("token-cache");

    TokenCache cache =
        TokenCacheUtils.createEncryptedCache(cachePath, null, TEST_HOST, TEST_CLIENT_ID);

    assertNotNull(cache, "Cache should not be null");
    assertInstanceOf(
        EncryptedFileTokenCache.class, cache, "Cache should be an EncryptedFileTokenCache");
  }

  @Test
  void testCreateEncryptedCache_WithEmptyPassphrase() {
    Path cachePath = tempDir.resolve("token-cache");

    TokenCache cache =
        TokenCacheUtils.createEncryptedCache(cachePath, "", TEST_HOST, TEST_CLIENT_ID);

    assertNotNull(cache, "Cache should not be null");
    assertInstanceOf(
        EncryptedFileTokenCache.class, cache, "Cache should be an EncryptedFileTokenCache");
  }

  @Test
  void testCreateEncryptedCache_CacheIsUsableForSaveAndLoad() throws Exception {
    Path cachePath = tempDir.resolve("token-cache");
    String passphrase = "test-passphrase";

    TokenCache cache =
        TokenCacheUtils.createEncryptedCache(cachePath, passphrase, TEST_HOST, TEST_CLIENT_ID);

    // Create and save a token
    Instant expiry = Instant.now().plus(1, ChronoUnit.HOURS);
    Token originalToken = new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, expiry);
    cache.save(originalToken);

    // Verify file was created
    assertTrue(Files.exists(cachePath), "Cache file should exist after save");

    // Load and verify token
    Token loadedToken = cache.load();
    assertNotNull(loadedToken, "Loaded token should not be null");
    assertEquals(ACCESS_TOKEN, loadedToken.getAccessToken(), "Access token should match");
  }

  @Test
  void testCreateEncryptedCache_DifferentPassphrasesShouldNotDecryptEachOther() throws Exception {
    Path cachePath = tempDir.resolve("token-cache");

    // Create cache with first passphrase and save a token
    TokenCache cache1 =
        TokenCacheUtils.createEncryptedCache(cachePath, "passphrase-1", TEST_HOST, TEST_CLIENT_ID);
    Instant expiry = Instant.now().plus(1, ChronoUnit.HOURS);
    Token originalToken = new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, expiry);
    cache1.save(originalToken);

    // Try to load with different passphrase
    TokenCache cache2 =
        TokenCacheUtils.createEncryptedCache(cachePath, "passphrase-2", TEST_HOST, TEST_CLIENT_ID);
    Token loadedToken = cache2.load();

    assertNull(loadedToken, "Token encrypted with different passphrase should not decrypt");
  }
}
