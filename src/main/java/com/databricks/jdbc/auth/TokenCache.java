package com.databricks.jdbc.auth;

import com.databricks.sdk.core.oauth.Token;
import com.databricks.sdk.core.utils.ClockSupplier;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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

  public TokenCache(String host, String passphrase) {
    if (passphrase == null || passphrase.isEmpty()) {
      throw new IllegalArgumentException(
          "Required setting TokenCachePassPhrase has not been provided in connection settings");
    }
    this.passphrase = passphrase;
    this.cacheFile =
        Paths.get(System.getProperty("java.io.tmpdir"), CACHE_DIR, host + CACHE_FILE_SUFFIX);
    this.mapper = new ObjectMapper();
    this.mapper.registerModule(new JavaTimeModule());
  }

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

  private SecretKey generateSecretKey() throws Exception {
    SecretKeyFactory factory = SecretKeyFactory.getInstance(SECRET_KEY_ALGORITHM);
    KeySpec spec = new PBEKeySpec(passphrase.toCharArray(), SALT, ITERATION_COUNT, KEY_LENGTH);
    return new SecretKeySpec(factory.generateSecret(spec).getEncoded(), ALGORITHM);
  }

  private byte[] encrypt(byte[] data) throws Exception {
    Cipher cipher = Cipher.getInstance(ALGORITHM);
    cipher.init(Cipher.ENCRYPT_MODE, generateSecretKey());
    return Base64.getEncoder().encode(cipher.doFinal(data));
  }

  private byte[] decrypt(byte[] encryptedData) throws Exception {
    Cipher cipher = Cipher.getInstance(ALGORITHM);
    cipher.init(Cipher.DECRYPT_MODE, generateSecretKey());
    return cipher.doFinal(Base64.getDecoder().decode(encryptedData));
  }
}
