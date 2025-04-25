package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.core.oauth.Token;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

public class NoOpTokenCacheTest {

  private static final String ACCESS_TOKEN = "test-access-token";
  private static final String REFRESH_TOKEN = "test-refresh-token";
  private static final String TOKEN_TYPE = "Bearer";

  @Test
  void testSaveDoesNothing() {
    // Create NoOpTokenCache instance
    NoOpTokenCache tokenCache = new NoOpTokenCache();

    // Create a token
    Token token =
        new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, LocalDateTime.now().plusHours(1));

    // Save should not throw any exception
    assertDoesNotThrow(() -> tokenCache.save(token), "save() should not throw an exception");
  }

  @Test
  void testLoadAlwaysReturnsNull() {
    // Create NoOpTokenCache instance
    NoOpTokenCache tokenCache = new NoOpTokenCache();

    // Load should always return null
    Token loadedToken = tokenCache.load();
    assertNull(loadedToken, "load() should always return null");
  }

  @Test
  void testSaveAndLoadSequence() {
    // Create NoOpTokenCache instance
    NoOpTokenCache tokenCache = new NoOpTokenCache();

    // Create a token
    Token token =
        new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, LocalDateTime.now().plusHours(1));

    // Save the token
    tokenCache.save(token);

    // Load should still return null even after save
    Token loadedToken = tokenCache.load();
    assertNull(loadedToken, "load() should return null even after save()");
  }
}
