package com.databricks.jdbc.auth;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.oauth.Token;
import com.databricks.sdk.core.oauth.TokenCache;
import com.databricks.sdk.core.utils.SerDeUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.security.spec.KeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
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
  private static final int ITERATION_COUNT = 65536;
  private static final int KEY_LENGTH = 256;
  private static final int IV_SIZE = 16; // 128 bits
  private static final int SALT_SIZE = 16; // random per-file salt

  private static final Set<PosixFilePermission> OWNER_ONLY =
      EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
  private static final Set<PosixFilePermission> OWNER_ONLY_DIR =
      EnumSet.of(
          PosixFilePermission.OWNER_READ,
          PosixFilePermission.OWNER_WRITE,
          PosixFilePermission.OWNER_EXECUTE);

  // Same tag as the SQL kernel's cache_crypto.rs, so both derive the same key for a user.
  private static final String DERIVED_PASSPHRASE_TAG = "databricks-sql-kernel/token-cache/v1";

  private final Path cacheFile;
  private final ObjectMapper mapper;
  private final String passphrase;

  public EncryptedFileTokenCache(Path cacheFilePath, String passphrase) {
    Objects.requireNonNull(cacheFilePath, "cacheFilePath must be defined");
    Objects.requireNonNull(passphrase, "passphrase must be defined for encrypted token cache");
    this.cacheFile = cacheFilePath;
    this.mapper = SerDeUtils.createMapper();
    this.passphrase = passphrase;
  }

  /**
   * Passphrase for a cache enabled without {@code TokenCachePassPhrase}, matching the SQL kernel
   * and ODBC driver: a fixed tag plus the OS user name ({@code USER}, {@code USERNAME}, then {@code
   * LOGNAME}; empty if none is set). It is obfuscation, not a secret: anyone who knows the user
   * name can derive it. The real local control is the owner-only file mode on POSIX file systems,
   * and the user profile's ACLs on Windows.
   */
  public static String derivedPassphrase() {
    return derivedPassphrase(System::getenv);
  }

  @VisibleForTesting
  static String derivedPassphrase(UnaryOperator<String> getenv) {
    String user = getenv.apply("USER");
    if (user == null) {
      user = getenv.apply("USERNAME");
    }
    if (user == null) {
      user = getenv.apply("LOGNAME");
    }
    return DERIVED_PASSPHRASE_TAG + '\u001f' + (user == null ? "" : user);
  }

  /** Best effort: a failed write is logged and never fails authentication or token refresh. */
  @Override
  public void save(Token token) throws DatabricksException {
    try {
      Path dir = cacheFile.getParent();
      Files.createDirectories(dir);
      restrictDirectoryToOwner(dir);
      String json = mapper.writeValueAsString(token);
      byte[] dataToWrite = encrypt(json.getBytes(StandardCharsets.UTF_8));
      writeOwnerOnly(cacheFile, dataToWrite);
      LOGGER.debug("Successfully saved encrypted token to cache: {}", cacheFile);
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to save token cache to {}: {}: {}",
          cacheFile,
          e.getClass().getSimpleName(),
          e.getMessage());
    }
  }

  /** Sets the cache directory to 0700 where POSIX permissions are supported, as the kernel does. */
  private static void restrictDirectoryToOwner(Path dir) {
    if (!dir.getFileSystem().supportedFileAttributeViews().contains("posix")) {
      return;
    }
    try {
      Files.setPosixFilePermissions(dir, OWNER_ONLY_DIR);
    } catch (IOException | RuntimeException e) {
      LOGGER.debug("Could not restrict token cache directory {}: {}", dir, e.getMessage());
    }
  }

  @Override
  public Token load() {
    try {
      if (!Files.exists(cacheFile)) {
        LOGGER.debug("No token cache file found at: {}", cacheFile);
        return null;
      }
      byte[] fileContent = Files.readAllBytes(cacheFile);
      Token token;
      try {
        token =
            mapper.readValue(new String(decrypt(fileContent), StandardCharsets.UTF_8), Token.class);
      } catch (Exception e) {
        // Logged at WARN like the kernel: otherwise repeated browser logins have no visible cause.
        LOGGER.warn(
            "Token cache at {} could not be decrypted (TokenCachePassPhrase or OS user name"
                + " changed, or the file is corrupt); ignoring it, the next login rewrites it",
            cacheFile);
        return null;
      }
      LOGGER.debug("Successfully loaded encrypted token from cache: {}", cacheFile);
      return token;
    } catch (Exception e) {
      LOGGER.debug("Failed to load token from cache: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Writes an owner-only temp file in the same directory, then moves it over the cache file, so a
   * concurrent load() sees the old or the new file, never a partly written one (as the kernel
   * does).
   */
  private void writeOwnerOnly(Path path, byte[] data) throws IOException {
    boolean posix = path.getFileSystem().supportedFileAttributeViews().contains("posix");
    String prefix = path.getFileName() + ".";
    Path tmp =
        posix
            ? Files.createTempFile(
                path.getParent(), prefix, ".tmp", PosixFilePermissions.asFileAttribute(OWNER_ONLY))
            : Files.createTempFile(path.getParent(), prefix, ".tmp");
    try {
      if (!posix) {
        File file = tmp.toFile();
        file.setReadable(false, false);
        file.setReadable(true, true);
        file.setWritable(false, false);
        file.setWritable(true, true);
      }
      Files.write(tmp, data);
      try {
        Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      try {
        Files.deleteIfExists(tmp);
      } catch (IOException e) {
        LOGGER.debug("Could not delete temporary token cache file {}: {}", tmp, e.getMessage());
      }
    }
  }

  /** Derives the AES key from the passphrase and the given (per-file) salt. */
  private SecretKey generateSecretKey(byte[] salt) throws Exception {
    SecretKeyFactory factory = SecretKeyFactory.getInstance(SECRET_KEY_ALGORITHM);
    KeySpec spec = new PBEKeySpec(passphrase.toCharArray(), salt, ITERATION_COUNT, KEY_LENGTH);
    return new SecretKeySpec(factory.generateSecret(spec).getEncoded(), ALGORITHM);
  }

  /** Output = Base64(salt || IV || ciphertext). Salt and IV are random per save. */
  private byte[] encrypt(byte[] data) throws Exception {
    SecureRandom random = new SecureRandom();
    byte[] salt = new byte[SALT_SIZE];
    random.nextBytes(salt);
    byte[] iv = new byte[IV_SIZE];
    random.nextBytes(iv);

    Cipher cipher = Cipher.getInstance(TRANSFORMATION);
    cipher.init(Cipher.ENCRYPT_MODE, generateSecretKey(salt), new IvParameterSpec(iv));
    byte[] encryptedData = cipher.doFinal(data);

    byte[] combined = new byte[salt.length + iv.length + encryptedData.length];
    System.arraycopy(salt, 0, combined, 0, salt.length);
    System.arraycopy(iv, 0, combined, salt.length, iv.length);
    System.arraycopy(encryptedData, 0, combined, salt.length + iv.length, encryptedData.length);
    return Base64.getEncoder().encode(combined);
  }

  /** Reads the per-file salt and IV back from the front of the payload, then decrypts. */
  private byte[] decrypt(byte[] encryptedData) throws Exception {
    byte[] decoded = Base64.getDecoder().decode(encryptedData);
    byte[] salt = Arrays.copyOfRange(decoded, 0, SALT_SIZE);
    byte[] iv = Arrays.copyOfRange(decoded, SALT_SIZE, SALT_SIZE + IV_SIZE);
    byte[] actualData = Arrays.copyOfRange(decoded, SALT_SIZE + IV_SIZE, decoded.length);

    Cipher cipher = Cipher.getInstance(TRANSFORMATION);
    cipher.init(Cipher.DECRYPT_MODE, generateSecretKey(salt), new IvParameterSpec(iv));
    return cipher.doFinal(actualData);
  }
}
