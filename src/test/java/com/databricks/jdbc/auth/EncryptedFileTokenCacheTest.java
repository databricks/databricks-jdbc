package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.oauth.Token;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
        new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, LocalDateTime.now().plusHours(1));

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
    assertFalse(loadedToken.isExpired(), "Token should not be expired");
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
        new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, LocalDateTime.now().plusHours(1));
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
        new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, LocalDateTime.now().plusHours(1));

    // Save the token
    tokenCache.save(token);

    // Verify the file exists
    assertTrue(Files.exists(tokenCachePath), "Token cache file should exist");

    // Verify file permissions (owner should have read/write)
    assertTrue(tokenCachePath.toFile().canRead(), "File should be readable by owner");
    assertTrue(tokenCachePath.toFile().canWrite(), "File should be writable by owner");
  }
}
