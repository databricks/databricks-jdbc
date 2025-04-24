package com.databricks.jdbc.auth;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.oauth.Token;
import com.databricks.sdk.core.oauth.TokenCache;
import com.databricks.sdk.core.utils.SerDeUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.spec.KeySpec;
import java.util.Base64;
import java.util.Objects;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/** A TokenCache implementation that stores tokens in encrypted files. */
public class EncryptedFileTokenCache implements TokenCache {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(EncryptedFileTokenCache.class);

  // Encryption constants
  private static final String ALGORITHM = "AES";
  private static final String TRANSFORMATION = "AES/CBC/PKCS5Padding";
  private static final String SECRET_KEY_ALGORITHM = "PBKDF2WithHmacSHA256";
  private static final byte[] SALT = "DatabricksJdbcTokenCache".getBytes();
  private static final int ITERATION_COUNT = 65536;
  private static final int KEY_LENGTH = 256;
  private static final int IV_SIZE = 16; // 128 bits

  private final Path cacheFile;
  private final ObjectMapper mapper;
  private final String passphrase;

  /**
   * Constructs a new EncryptingFileTokenCache instance.
   *
   * @param cacheFilePath The path where the token cache will be stored
   * @param passphrase The passphrase used for encryption
   */
  public EncryptedFileTokenCache(Path cacheFilePath, String passphrase) {
    Objects.requireNonNull(cacheFilePath, "cacheFilePath must be defined");
    Objects.requireNonNull(passphrase, "passphrase must be defined for encrypted token cache");

    this.cacheFile = cacheFilePath;
    this.mapper = SerDeUtils.createMapper();
    this.passphrase = passphrase;
  }

  @Override
  public void save(Token token) throws DatabricksException {
    try {
      Files.createDirectories(cacheFile.getParent());

      // Serialize token to JSON
      String json = mapper.writeValueAsString(token);
      byte[] dataToWrite = json.getBytes(StandardCharsets.UTF_8);

      // Encrypt data
      dataToWrite = encrypt(dataToWrite);

      Files.write(cacheFile, dataToWrite);
      // Set file permissions to be readable only by the owner (equivalent to 0600)
      File file = cacheFile.toFile();
      file.setReadable(false, false);
      file.setReadable(true, true);
      file.setWritable(false, false);
      file.setWritable(true, true);

      LOGGER.debug("Successfully saved encrypted token to cache: %s", cacheFile);
    } catch (Exception e) {
      throw new DatabricksException("Failed to save token cache: " + e.getMessage(), e);
    }
  }

  @Override
  public Token load() {
    try {
      if (!Files.exists(cacheFile)) {
        LOGGER.debug("No token cache file found at: %s", cacheFile);
        return null;
      }

      byte[] fileContent = Files.readAllBytes(cacheFile);

      // Decrypt data
      byte[] decodedContent;
      try {
        decodedContent = decrypt(fileContent);
      } catch (Exception e) {
        LOGGER.debug("Failed to decrypt token cache: %s", e.getMessage());
        return null;
      }

      // Deserialize token from JSON
      String json = new String(decodedContent, StandardCharsets.UTF_8);
      Token token = mapper.readValue(json, Token.class);
      LOGGER.debug("Successfully loaded encrypted token from cache: %s", cacheFile);
      return token;
    } catch (Exception e) {
      // If there's any issue loading the token, return null
      // to allow a fresh token to be obtained
      LOGGER.debug("Failed to load token from cache: %s", e.getMessage());
      return null;
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
   * Encrypts the given data using AES/CBC/PKCS5Padding encryption with a key derived from the
   * passphrase. The IV is generated randomly and prepended to the encrypted data.
   *
   * @param data The data to encrypt
   * @return The encrypted data with IV prepended
   * @throws Exception If an error occurs during encryption
   */
  private byte[] encrypt(byte[] data) throws Exception {
    Cipher cipher = Cipher.getInstance(TRANSFORMATION);

    // Generate a random IV
    SecureRandom random = new SecureRandom();
    byte[] iv = new byte[IV_SIZE];
    random.nextBytes(iv);
    IvParameterSpec ivSpec = new IvParameterSpec(iv);

    cipher.init(Cipher.ENCRYPT_MODE, generateSecretKey(), ivSpec);
    byte[] encryptedData = cipher.doFinal(data);

    // Combine IV and encrypted data
    byte[] combined = new byte[iv.length + encryptedData.length];
    System.arraycopy(iv, 0, combined, 0, iv.length);
    System.arraycopy(encryptedData, 0, combined, iv.length, encryptedData.length);

    return Base64.getEncoder().encode(combined);
  }

  /**
   * Decrypts the given encrypted data using AES/CBC/PKCS5Padding decryption with a key derived from
   * the passphrase. The IV is extracted from the beginning of the encrypted data.
   *
   * @param encryptedData The encrypted data with IV prepended, Base64 encoded
   * @return The decrypted data
   * @throws Exception If an error occurs during decryption
   */
  private byte[] decrypt(byte[] encryptedData) throws Exception {
    byte[] decodedData = Base64.getDecoder().decode(encryptedData);

    // Extract IV
    byte[] iv = new byte[IV_SIZE];
    byte[] actualData = new byte[decodedData.length - IV_SIZE];
    System.arraycopy(decodedData, 0, iv, 0, IV_SIZE);
    System.arraycopy(decodedData, IV_SIZE, actualData, 0, actualData.length);

    Cipher cipher = Cipher.getInstance(TRANSFORMATION);
    IvParameterSpec ivSpec = new IvParameterSpec(iv);
    cipher.init(Cipher.DECRYPT_MODE, generateSecretKey(), ivSpec);

    return cipher.doFinal(actualData);
  }
}
