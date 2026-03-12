package com.databricks.jdbc.auth;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.interfaces.RSAPrivateKey;
import java.util.Base64;

/** Utility class for generating temporary private keys for testing JWT authentication. */
public class TestKeyGenerator {

  /**
   * Generates a temporary RSA private key file in PEM format for testing.
   *
   * @return Path to the temporary key file
   * @throws Exception if key generation or file writing fails
   */
  public static Path generateTemporaryKeyFile() throws Exception {
    // Generate a temporary RSA key pair for testing
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048, new SecureRandom());
    KeyPair keyPair = keyGen.generateKeyPair();
    RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();

    // Create temporary file
    Path tempKeyFile = Files.createTempFile("test-private-key-", ".pem");

    // Write private key in PKCS#8 PEM format (Java's default encoding)
    try (FileWriter writer = new FileWriter(tempKeyFile.toFile())) {
      writer.write("-----BEGIN PRIVATE KEY-----\n");
      String encoded =
          Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(privateKey.getEncoded());
      writer.write(encoded);
      writer.write("\n-----END PRIVATE KEY-----\n");
    }

    return tempKeyFile;
  }

  /**
   * Deletes a temporary key file created by generateTemporaryKeyFile.
   *
   * @param keyFile Path to the key file to delete
   * @throws Exception if file deletion fails
   */
  public static void cleanupKeyFile(Path keyFile) throws Exception {
    if (keyFile != null) {
      Files.deleteIfExists(keyFile);
    }
  }
}
