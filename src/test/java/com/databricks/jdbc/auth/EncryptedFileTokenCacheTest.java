package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.oauth.Token;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
    String longToken = "a".repeat(2000);
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

  @Test
  void should_NotBeDecryptableWithHardcodedSalt() throws Exception {
    EncryptedFileTokenCache cache = new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);
    cache.save(
        new Token(
            ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS)));

    byte[] combined = Base64.getDecoder().decode(Files.readAllBytes(tokenCachePath));
    byte[] iv = Arrays.copyOfRange(combined, 0, 16);
    byte[] ct = Arrays.copyOfRange(combined, 16, combined.length);
    javax.crypto.SecretKeyFactory f =
        javax.crypto.SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
    javax.crypto.SecretKey key =
        new javax.crypto.spec.SecretKeySpec(
            f.generateSecret(
                    new javax.crypto.spec.PBEKeySpec(
                        TEST_PASSPHRASE.toCharArray(),
                        "DatabricksJdbcTokenCache".getBytes(),
                        65536,
                        256))
                .getEncoded(),
            "AES");
    boolean cracked;
    try {
      javax.crypto.Cipher c = javax.crypto.Cipher.getInstance("AES/CBC/PKCS5Padding");
      c.init(javax.crypto.Cipher.DECRYPT_MODE, key, new javax.crypto.spec.IvParameterSpec(iv));
      String out = new String(c.doFinal(ct));
      cracked = out.contains(REFRESH_TOKEN);
    } catch (Exception e) {
      cracked = false;
    }
    assertFalse(cracked, "Token must not be derivable from passphrase + hardcoded salt");
  }

  @Test
  void should_UseRandomPerFileSalt() throws Exception {
    Path p1 = tempDir.resolve("c1");
    Path p2 = tempDir.resolve("c2");
    Token t =
        new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS));
    new EncryptedFileTokenCache(p1, TEST_PASSPHRASE).save(t);
    new EncryptedFileTokenCache(p2, TEST_PASSPHRASE).save(t);
    byte[] a = Arrays.copyOfRange(Base64.getDecoder().decode(Files.readAllBytes(p1)), 0, 16);
    byte[] b = Arrays.copyOfRange(Base64.getDecoder().decode(Files.readAllBytes(p2)), 0, 16);
    assertFalse(Arrays.equals(a, b), "Salt (first 16 bytes) must differ per file");
  }

  @Test
  void should_LoadFromFreshInstance_SamePassphrase() throws Exception {
    Token t =
        new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS));
    new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE).save(t);
    Token loaded = new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE).load();
    assertNotNull(loaded);
    assertEquals(REFRESH_TOKEN, loaded.getRefreshToken());
  }

  @Test
  void should_WriteOwnerOnlyPermissions() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    EncryptedFileTokenCache cache = new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);
    cache.save(
        new Token(
            ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS)));
    assertEquals(
        PosixFilePermissions.fromString("rw-------"),
        Files.getPosixFilePermissions(tokenCachePath));
  }

  @Test
  void should_KeepOwnerOnlyPermissions_AfterOverwriteAndFromLoosePreexisting() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    Files.write(tokenCachePath, new byte[] {1, 2, 3});
    Files.setPosixFilePermissions(tokenCachePath, PosixFilePermissions.fromString("rw-r--r--"));
    EncryptedFileTokenCache cache = new EncryptedFileTokenCache(tokenCachePath, TEST_PASSPHRASE);
    cache.save(
        new Token(
            ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS)));
    assertEquals(
        PosixFilePermissions.fromString("rw-------"),
        Files.getPosixFilePermissions(tokenCachePath),
        "overwrite must normalize to 0600");
  }

  private static final String KERNEL_TAG = "databricks-sql-kernel/token-cache/v1\u001f";

  @Test
  void derivedPassphrase_UsesFirstSetOfUserUsernameLogname() {
    Map<String, String> env = new HashMap<>();
    assertEquals(KERNEL_TAG, EncryptedFileTokenCache.derivedPassphrase(env::get));
    env.put("LOGNAME", "from-logname");
    assertEquals(KERNEL_TAG + "from-logname", EncryptedFileTokenCache.derivedPassphrase(env::get));
    env.put("USERNAME", "from-username");
    assertEquals(KERNEL_TAG + "from-username", EncryptedFileTokenCache.derivedPassphrase(env::get));
    env.put("USER", "from-user");
    assertEquals(KERNEL_TAG + "from-user", EncryptedFileTokenCache.derivedPassphrase(env::get));
    // A set-but-empty USER is used as-is, like the kernel's std::env::var.
    env.put("USER", "");
    assertEquals(KERNEL_TAG, EncryptedFileTokenCache.derivedPassphrase(env::get));
  }

  @Test
  void derivedPassphrase_ReadsProcessEnvironment() {
    assertEquals(
        EncryptedFileTokenCache.derivedPassphrase(System::getenv),
        EncryptedFileTokenCache.derivedPassphrase());
  }

  /**
   * Payloads from the SQL kernel's src/auth/oauth/cache_crypto.rs (databricks-sql-kernel@375cdb6,
   * compiled unmodified): encrypt(plaintext, derived_passphrase()) with only the given env var set.
   */
  @ParameterizedTest
  @CsvSource({
    "USER,jdbc-fixture-user,o5MGqx4C/6QzrXv/LqzSJmLatxzYU4Tt29Mb8rAxLbCJtK/s1PW74fA+JEVlFsqxhG+WVk+n9ImCPtOzX6Q1pPgbah6uOlO0kgH6VHtZGOwODgJshUICuHZbmbQlDIFN6Abq6/Rl4Lbj3mEf2vFyvAlwQfXGEM+1aXywn1yv36N/HS84oXVlEuC+j3TMvZyehrQARTIXk6P00nulPXXLMQ==",
    "USER,jösé-ユーザー,oF5A6xMiGZeHBqF3M7SFtpI1c6NurJ6gNsuq7CA5VDdAjKcpPanwVjZID1uMZsnt2z3UF+mJBkb7kXUKg/5/Sn6ucanVlWG8sNapFCI135FoUtMpjLvzXuZSah43F2WASwumb4yIvlJ5y36GQQ6cEShJf/XFF9rBFRgkyuL4iCO1U13EAeoFxVUDQY26kHEjFJL+WjKK/yQK6T+c0LTCKw==",
    "USERNAME,win-fixture-user,63xHudJGJFg8/d/BaE8oXNCpB3DhUol1we37FJXlJJm/oECcQuW72lZdaqTDKpWvF87PE3qRgRXoiucosvyltIEsvGXiCikwl9G1N5bM5buzJKAOtgMBtYUIkhaNPNNCqrSBvXcmfAaQBf89RCvNrd8srEfpyTpQGekt/4wRczjU16quKQkn41ZNtvMc3nvdYuRhnIWZtPOC8SentfdBcQ==",
    "NONE,,26ODVLH7m0/REl7FylHyp6mXE7e8HG1VfuDzZ1vWwF+6so2mLyafRVwQxF/5a5woGwFGpvaeGcO3IupY+h7dYsTpajGiNQ1Uj486vcWvHVccbLyrrRxNLJ57NHClOvqKaYf1LCVnDv5mesx0+aq8VfzdZDu3BDKodKva8n92Xx1IIKd3btZf/UtDiSuuiNc5h+s3exfkx/eHmMCaqvpyWg=="
  })
  void should_DecryptKernelPayload_WithDerivedPassphrase(String var, String user, String payload)
      throws Exception {
    Map<String, String> env = new HashMap<>();
    if (user != null) {
      env.put(var, user);
    }
    Files.write(tokenCachePath, payload.getBytes(StandardCharsets.US_ASCII));

    Token token =
        new EncryptedFileTokenCache(
                tokenCachePath, EncryptedFileTokenCache.derivedPassphrase(env::get))
            .load();

    assertNotNull(token, "kernel payload must decrypt with the JDBC-derived passphrase");
    assertEquals("at-kernel-fixture", token.getAccessToken());
    assertEquals("rt-kernel-fixture", token.getRefreshToken());
    assertEquals(Instant.parse("2030-01-01T00:00:00Z"), token.getExpiry());
    env.put("USER", "someone-else");
    assertNull(
        new EncryptedFileTokenCache(
                tokenCachePath, EncryptedFileTokenCache.derivedPassphrase(env::get))
            .load(),
        "another user's derived key must not decrypt it");
  }

  @Test
  void should_TreatUndecryptableCacheAsMiss_AndOverwriteOnSave() {
    Token old =
        new Token("old", TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS));
    new EncryptedFileTokenCache(tokenCachePath, "explicit-passphrase").save(old);
    EncryptedFileTokenCache derived =
        new EncryptedFileTokenCache(tokenCachePath, EncryptedFileTokenCache.derivedPassphrase());

    assertNull(derived.load(), "a file written under another passphrase is a cache miss");

    derived.save(
        new Token("new", TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS)));
    assertEquals("new", derived.load().getAccessToken());
  }

  @Test
  void should_NotThrow_When_SaveFails() throws Exception {
    Path notADirectory = tempDir.resolve("file");
    Files.write(notADirectory, new byte[] {1});
    EncryptedFileTokenCache cache =
        new EncryptedFileTokenCache(notADirectory.resolve("token-cache"), TEST_PASSPHRASE);

    assertDoesNotThrow(
        () ->
            cache.save(
                new Token(
                    ACCESS_TOKEN,
                    TOKEN_TYPE,
                    REFRESH_TOKEN,
                    Instant.now().plus(1, ChronoUnit.HOURS))));
    assertNull(cache.load());
  }

  @Test
  void should_RestrictCacheDirectoryToOwner() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    Path existingDir = Files.createDirectory(tempDir.resolve("existing"));
    Files.setPosixFilePermissions(existingDir, PosixFilePermissions.fromString("rwxr-xr-x"));
    Path newDir = tempDir.resolve("new/oauth");
    Token token =
        new Token(ACCESS_TOKEN, TOKEN_TYPE, REFRESH_TOKEN, Instant.now().plus(1, ChronoUnit.HOURS));

    new EncryptedFileTokenCache(existingDir.resolve("token-cache"), TEST_PASSPHRASE).save(token);
    new EncryptedFileTokenCache(newDir.resolve("token-cache"), TEST_PASSPHRASE).save(token);

    assertEquals(
        PosixFilePermissions.fromString("rwx------"), Files.getPosixFilePermissions(existingDir));
    assertEquals(
        PosixFilePermissions.fromString("rwx------"), Files.getPosixFilePermissions(newDir));
  }
}
