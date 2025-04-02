package com.databricks.jdbc.auth;

import com.databricks.jdbc.common.util.StringUtil;
import com.databricks.sdk.core.oauth.Token;
import com.databricks.sdk.core.utils.ClockSupplier;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.spec.KeySpec;
import java.time.LocalDateTime;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * A secure cache for OAuth tokens that encrypts and persists tokens to the local filesystem.
 *
 * <p>This class provides functionality to securely store and retrieve OAuth tokens between
 * application sessions. Tokens are encrypted using AES encryption with a key derived from the
 * provided passphrase using PBKDF2 key derivation.
 *
 * <p>The tokens are stored in a file within the system's temporary directory under a '.databricks'
 * subdirectory. Each user has their own token cache file identified by the sanitized username.
 */
public class TokenCache {
  private static final String CACHE_DIR = ".databricks";
  private static final String CACHE_FILE_SUFFIX = ".databricks_jdbc_token_cache";
  private static final String ALGORITHM = "AES";
  private static final String SECRET_KEY_ALGORITHM = "PBKDF2WithHmacSHA256";
  private static final byte[] SALT = "DatabricksTokenCache".getBytes(); // Fixed salt for simplicity
  private static final int ITERATION_COUNT = 65536;
  private static final int KEY_LENGTH = 256;

  private final Path cacheFile;
  private final String passphrase;
  private final ObjectMapper mapper;

  /**
   * A serializable version of the Token class that can be serialized/deserialized by Jackson. This
   * class extends the Token class from the SDK and adds JSON annotations for proper serialization.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class SerializableToken extends Token {
    public SerializableToken(String accessToken, String tokenType, LocalDateTime expiry) {
      super(accessToken, tokenType, expiry);
    }

    public SerializableToken(
        String accessToken, String tokenType, LocalDateTime expiry, ClockSupplier clockSupplier) {
      super(accessToken, tokenType, expiry, clockSupplier);
    }

    @JsonCreator
    public SerializableToken(
        @JsonProperty("accessToken") String accessToken,
        @JsonProperty("tokenType") String tokenType,
        @JsonProperty("refreshToken") String refreshToken,
        @JsonProperty("expiry") LocalDateTime expiry) {
      super(accessToken, tokenType, refreshToken, expiry);
    }

    public SerializableToken(
        String accessToken,
        String tokenType,
        String refreshToken,
        LocalDateTime expiry,
        ClockSupplier clockSupplier) {
      super(accessToken, tokenType, refreshToken, expiry, clockSupplier);
    }
  }

  /**
   * Constructs a new TokenCache instance with encryption using the user's system username to
   * identify the cache file.
   *
   * @param passphrase The passphrase used to encrypt/decrypt the token cache
   * @throws IllegalArgumentException if the passphrase is null or empty
   */
  public TokenCache(String passphrase) {
    this(
        Paths.get(
            System.getProperty("java.io.tmpdir"),
            CACHE_DIR,
            StringUtil.sanitizeUsernameForFile(System.getProperty("user.name"))
                + CACHE_FILE_SUFFIX),
        passphrase);
  }

  /**
   * Constructs a new TokenCache instance with encryption using the cache file path provided.
   *
   * @param cacheFile The cache file path
   * @param passphrase The passphrase used to encrypt/decrypt the token cache
   */
  @VisibleForTesting
  public TokenCache(Path cacheFile, String passphrase) {
    if (passphrase == null || passphrase.isEmpty()) {
      throw new IllegalArgumentException(
          "Required setting TokenCachePassPhrase has not been provided in connection settings");
    }
    this.passphrase = passphrase;
    this.cacheFile = cacheFile;
    this.mapper = new ObjectMapper();
    this.mapper.registerModule(new JavaTimeModule());
  }

  /**
   * Saves a token to the cache file, encrypting it with the configured passphrase.
   *
   * @param token The token to save to the cache
   * @throws IOException If an error occurs writing the token to the file or during encryption
   */
  public void save(Token token) throws IOException {
    try {
      Files.createDirectories(cacheFile.getParent());
      String json = mapper.writeValueAsString(token);
      byte[] encrypted = encrypt(json.getBytes());
      Files.write(cacheFile, encrypted);
    } catch (Exception e) {
      throw new IOException("Failed to save token cache: " + e.getMessage(), e);
    }
  }

  /**
   * Loads a token from the cache file, decrypting it with the configured passphrase.
   *
   * @return The decrypted token from the cache or null if the cache file doesn't exist
   * @throws IOException If an error occurs reading the token from the file or during decryption
   */
  public Token load() throws IOException {
    try {
      if (!Files.exists(cacheFile)) {
        return null;
      }
      byte[] encrypted = Files.readAllBytes(cacheFile);
      byte[] decrypted = decrypt(encrypted);
      return mapper.readValue(decrypted, SerializableToken.class);
    } catch (Exception e) {
      throw new IOException("Failed to load token cache: " + e.getMessage(), e);
    }
  }

  /**
   * Generates a secret key from the passphrase using PBKDF2 with HMAC-SHA256.
   *
   * @return A SecretKey generated from the passphrase
   * @throws Exception If an error occurs generating the key
   */
  private SecretKey generateSecretKey() throws Exception {
    SecretKeyFactory factory = SecretKeyFactory.getInstance(SECRET_KEY_ALGORITHM);
    KeySpec spec = new PBEKeySpec(passphrase.toCharArray(), SALT, ITERATION_COUNT, KEY_LENGTH);
    return new SecretKeySpec(factory.generateSecret(spec).getEncoded(), ALGORITHM);
  }

  /**
   * Encrypts the given data using AES encryption with a key derived from the passphrase.
   *
   * @param data The data to encrypt
   * @return The encrypted data, Base64 encoded
   * @throws Exception If an error occurs during encryption
   */
  private byte[] encrypt(byte[] data) throws Exception {
    Cipher cipher = Cipher.getInstance(ALGORITHM);
    cipher.init(Cipher.ENCRYPT_MODE, generateSecretKey());
    return Base64.getEncoder().encode(cipher.doFinal(data));
  }

  /**
   * Decrypts the given encrypted data using AES decryption with a key derived from the passphrase.
   *
   * @param encryptedData The encrypted data, Base64 encoded
   * @return The decrypted data
   * @throws Exception If an error occurs during decryption
   */
  private byte[] decrypt(byte[] encryptedData) throws Exception {
    Cipher cipher = Cipher.getInstance(ALGORITHM);
    cipher.init(Cipher.DECRYPT_MODE, generateSecretKey());
    return cipher.doFinal(Base64.getDecoder().decode(encryptedData));
  }
}
