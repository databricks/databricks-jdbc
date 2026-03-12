package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.oauth.Token;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class EncryptedFileTokenCacheTest {

  @TempDir Path tempDir;

  private Path tokenCachePath;
  private static final String TEST_PASSPHRASE = "test-passphrase";
  private static final String ACCESS_TOKEN = "test-access-token";
  private static final String REFRESH_TOKEN = "test-refresh-token";
  private static final String TOKEN_TYPE = "Bearer";

  @BeforeEach
  void setUp() {
    tokenCachePath = tempDir.resolve("token-cache");
  }

  @Test
  void testSaveAndLoadToken() throws DatabricksException {
    // Create a token cache
    EncryptedFileTokenCache tokenCache =
        new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);

    // Create a token to save
    Token token =
        new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS));

    // Save the token
    tokenCache.save(token);

    // Verify the file exists
    assertTrue(Files.exists(tokenCachePath), "Token cache file should exist");

    // Load the token
    Token loadedToken = tokenCache.load();

    // Verify the loaded token matches the original
    assertNotNull(loadedToken, "Loaded token should not be null");
    assertEquals(ACCESS_TOKEN, loadedToken.getAccessToken(), "Access token should match");
    assertEquals(REFRESH_TOKEN, loadedToken.getRefreshToken(), "Refresh token should match");
    assertEquals(TOKEN_TYPE, loadedToken.getTokenType(), "Token type should match");
    assertFalse(loadedToken.getExpiry().isBefore(Instant.now()), "Token should not be expired");
  }

  @Test
  void testLoadNonExistentFile() {
    // Create token cache pointing to a non-existent file
    EncryptedFileTokenCache tokenCache =
        new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);

    // Attempt to load token from non-existent file
    Token token = tokenCache.load();

    // Verify null is returned
    assertNull(token, "Token should be null for non-existent cache file");
  }

  @Test
  void testDifferentPassphrase() throws DatabricksException {
    // Create a token cache with one passphrase
    EncryptedFileTokenCache tokenCache1 =
        new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);

    // Create and save a token
    Token token =
        new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS));
    tokenCache1.save(token);

    // Create a second token cache with a different passphrase
    EncryptedFileTokenCache tokenCache2 =
        new EncryptedFileTokenCache(tokenCachePath, "different-passphrase");

    // Attempt to load the token
    Token loadedToken = tokenCache2.load();

    // Verify null is returned due to decryption failure
    assertNull(loadedToken, "Token should be null when decryption fails");
  }

  @Test
  void testSaveWithNullParameters() {
    // Test with null path
    assertThrows(
        NullPointerException.class,
        () -> new EncryptedFileTokenCache(null, TEST_PASSPHRASE),
        "Should throw NullPointerException for null path");

    // Test with null passphrase
    assertThrows(
        NullPointerException.class,
        () -> new EncryptedFileTokenCache(tokenCachePath, null),
        "Should throw NullPointerException for null passphrase");
  }

  @Test
  void testFilePermissions() throws DatabricksException {
    // Create a token cache
    EncryptedFileTokenCache tokenCache =
        new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);

    // Create a token to save
    Token token =
        new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS));

    // Save the token
    tokenCache.save(token);

    // Verify the file exists
    assertTrue(Files.exists(tokenCachePath), "Token cache file should exist");

    // Verify file permissions (owner should have read/write)
    assertTrue(tokenCachePath.toFile().canRead(), "File should be readable by owner");
    assertTrue(tokenCachePath.toFile().canWrite(), "File should be writable by owner");
  }

  @Test
  void should_CreateParentDirectories_When_Saving() throws Exception {
    // Create a nested path that doesn't exist
    Path nestedPath = tempDir.resolve("nested/path/token-cache");

    EncryptedFileTokenCache tokenCache = new EncryptedFileTokenCache(nestedPath, TEST_PASSPHRASE);

    Token token =
        new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS));

    // Save should create parent directories
    tokenCache.save(token);

    // Verify the file exists
    assertTrue(Files.exists(nestedPath), "Token cache file should exist");
    assertTrue(Files.exists(nestedPath.getParent()), "Parent directory should be created");

    // Verify we can load it back
    Token loadedToken = tokenCache.load();
    assertNotNull(loadedToken);
    assertEquals(ACCESS_TOKEN, loadedToken.getAccessToken());
  }

  @Test
  void should_OverwriteExistingToken_When_SavingAgain() throws DatabricksException {
    EncryptedFileTokenCache tokenCache =
        new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);

    // Save first token
    Token token1 =
        new Token(
            "first-token", TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS));
    tokenCache.save(token1);

    // Save second token (should overwrite)
    Token token2 =
        new Token(
            "second-token", TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(2, ChronoUnit.HOURS));
    tokenCache.save(token2);

    // Load should return the second token
    Token loadedToken = tokenCache.load();
    assertNotNull(loadedToken);
    assertEquals("second-token", loadedToken.getAccessToken());
  }

  @Test
  void should_HandleTokenWithoutRefreshToken() throws DatabricksException {
    EncryptedFileTokenCache tokenCache =
        new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);

    // Create token without refresh token
    Token token =
        new Token(ACCESS_TOKEN, TOKEN_TYPE, null, Instant.now().plus(1, ChronoUnit.HOURS));

    tokenCache.save(token);

    Token loadedToken = tokenCache.load();
    assertNotNull(loadedToken);
    assertEquals(ACCESS_TOKEN, loadedToken.getAccessToken());
    assertNull(loadedToken.getRefreshToken());
  }

  @Test
  void should_HandleExpiredToken() throws DatabricksException {
    EncryptedFileTokenCache tokenCache =
        new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);

    // Create an expired token
    Token expiredToken =
        new Token(
            ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, Instant.now().minus(1, ChronoUnit.HOURS));

    tokenCache.save(expiredToken);

    Token loadedToken = tokenCache.load();
    assertNotNull(loadedToken);
    assertEquals(ACCESS_TOKEN, loadedToken.getAccessToken());
    assertTrue(loadedToken.getExpiry().isBefore(Instant.now()), "Token should be expired");
  }

  @Test
  void should_HandleVeryLongAccessToken() throws DatabricksException {
    EncryptedFileTokenCache tokenCache =
        new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);

    // Create a very long access token (simulate JWT)
    String longToken = new String(new char[2000]).replace('\0', 'a');
    Token token =
        new Token(longToken, TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS));

    tokenCache.save(token);

    Token loadedToken = tokenCache.load();
    assertNotNull(loadedToken);
    assertEquals(longToken, loadedToken.getAccessToken());
  }

  @Test
  void should_ReturnNull_When_CacheFileIsCorrupted() throws Exception {
    EncryptedFileTokenCache tokenCache =
        new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);

    // Write corrupted data to the cache file
    Files.write(tokenCachePath, "corrupted data".getBytes());

    // Load should return null for corrupted file
    Token token = tokenCache.load();
    assertNull(token, "Should return null for corrupted cache file");
  }

  @Test
  void should_HandleEmptyCacheFile() throws Exception {
    EncryptedFileTokenCache tokenCache =
        new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);

    // Create an empty cache file
    Files.write(tokenCachePath, new byte[0]);

    // Load should return null for empty file
    Token token = tokenCache.load();
    assertNull(token, "Should return null for empty cache file");
  }

  @Test
  void should_HandleMultipleSaveAndLoadCycles() throws DatabricksException {
    EncryptedFileTokenCache tokenCache =
        new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);

    // Perform multiple save/load cycles
    for (int i = 0; i < 5; i++) {
      Token token =
          new Token(
              "token-" + i, TOKEN_TYPE, "refresh-" + i, Instant.now().plus(i, ChronoUnit.HOURS));
      tokenCache.save(token);

      Token loadedToken = tokenCache.load();
      assertNotNull(loadedToken);
      assertEquals("token-" + i, loadedToken.getAccessToken());
      assertEquals("refresh-" + i, loadedToken.getRefreshToken());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"Bearer", "Basic", "Custom"})
  void should_HandleDifferentTokenTypes(String tokenType) throws DatabricksException {
    EncryptedFileTokenCache tokenCache =
        new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);

    Token token =
        new Token(ACCESS_TOKEN, tokenType, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS));
    tokenCache.save(token);

    Token loadedToken = tokenCache.load();
    assertNotNull(loadedToken);
    assertEquals(tokenType, loadedToken.getTokenType());
  }
}
