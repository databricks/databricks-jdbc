package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.core.oauth.Token;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TokenCacheTest {
  private static final String TEST_PASSPHRASE = "test-passphrase";
  private Path cacheFile;
  private TokenCache tokenCache;

  @BeforeEach
  void setUp() {
    tokenCache = new TokenCache(TEST_PASSPHRASE);
    String sanitizedUsername = System.getProperty("user.name").replaceAll("[^a-zA-Z0-9_]", "_");
    cacheFile =
        Paths.get(
            System.getProperty("java.io.tmpdir"),
            ".databricks",
            sanitizedUsername + ".databricks_jdbc_token_cache");
  }

  @AfterEach
  void tearDown() throws IOException {
    Files.deleteIfExists(cacheFile);
  }

  @Test
  void testEmptyCache() throws IOException {
    assertNull(tokenCache.load());
  }

  @Test
  void testSaveAndLoadToken() throws IOException {
    LocalDateTime expiry = LocalDateTime.now().plusHours(1);
    Token token = new Token("access-token", "Bearer", "refresh-token", expiry);

    tokenCache.save(token);
    Token loadedToken = tokenCache.load();

    assertNotNull(loadedToken);
    assertEquals("access-token", loadedToken.getAccessToken());
    assertEquals("Bearer", loadedToken.getTokenType());
    assertEquals("refresh-token", loadedToken.getRefreshToken());
    assertFalse(loadedToken.isExpired());
  }

  @Test
  void testInvalidPassphrase() {
    assertThrows(IllegalArgumentException.class, () -> new TokenCache(null));
    assertThrows(IllegalArgumentException.class, () -> new TokenCache(""));
  }

  @Test
  void testOverwriteToken() throws IOException {
    Token token1 = new Token("token1", "Bearer", "refresh1", LocalDateTime.now().plusHours(1));
    Token token2 = new Token("token2", "Bearer", "refresh2", LocalDateTime.now().plusHours(2));

    tokenCache.save(token1);
    tokenCache.save(token2);

    Token loadedToken = tokenCache.load();
    assertEquals("token2", loadedToken.getAccessToken());
    assertEquals("refresh2", loadedToken.getRefreshToken());
  }
}
