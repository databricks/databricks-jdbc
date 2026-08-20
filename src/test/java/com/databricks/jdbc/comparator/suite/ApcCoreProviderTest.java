package com.databricks.jdbc.comparator.suite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ApcCoreProviderTest {

  @TempDir Path tempDir;

  @AfterEach
  void clearManifestProperty() {
    System.clearProperty(ApcCustomTestManifest.PROPERTY);
  }

  @Test
  void appendsAcceptedCustomCases() throws Exception {
    Path manifest =
        writeManifest(
            "{\"schema_version\":1,\"cases\":[{\"id\":\"string-shape\","
                + "\"title\":\"String shape\",\"sql\":\"SELECT 'ok' AS value\"}]}");
    System.setProperty(ApcCustomTestManifest.PROPERTY, manifest.toString());

    List<TestCase> testCases = new ApcCoreProvider().getTestCases();

    assertEquals(5, testCases.size());
    assertEquals("[string-shape] String shape", testCases.get(4).getDescription());
    assertEquals("SELECT 'ok' AS value", testCases.get(4).getIdentifier());
  }

  @Test
  void rejectsUnsafeCustomSql() throws Exception {
    Path manifest =
        writeManifest(
            "{\"schema_version\":1,\"cases\":[{\"id\":\"unsafe\","
                + "\"title\":\"Unsafe\",\"sql\":\"DROP TABLE important\"}]}");
    System.setProperty(ApcCustomTestManifest.PROPERTY, manifest.toString());

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> new ApcCoreProvider().getTestCases());

    assertTrue(error.getMessage().contains("SQL must be one SELECT statement"));
  }

  @Test
  void rejectsExcludedMetadataSql() throws Exception {
    Path manifest =
        writeManifest(
            "{\"schema_version\":1,\"cases\":[{\"id\":\"metadata\",\"title\":\"Metadata\",\"sql\":\"SELECT"
                + " * FROM system.information_schema.tables\"}]}");
    System.setProperty(ApcCustomTestManifest.PROPERTY, manifest.toString());

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> new ApcCoreProvider().getTestCases());

    assertTrue(error.getMessage().contains("excluded metadata surfaces"));
  }

  @Test
  void rejectsChangedManifestShape() throws Exception {
    Path manifest =
        writeManifest(
            "{\"schema_version\":1,\"cases\":[{\"id\":\"extra-field\","
                + "\"title\":\"Extra field\",\"sql\":\"SELECT 1\",\"expected\":1}]}");
    System.setProperty(ApcCustomTestManifest.PROPERTY, manifest.toString());

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> new ApcCoreProvider().getTestCases());

    assertTrue(error.getMessage().contains("case fields must be exactly"));
  }

  @Test
  void rejectsDuplicateJsonFields() throws Exception {
    Path manifest =
        writeManifest(
            "{\"schema_version\":1,\"schema_version\":1,\"cases\":[{\"id\":\"duplicate\","
                + "\"title\":\"Duplicate\",\"sql\":\"SELECT 1\"}]}");
    System.setProperty(ApcCustomTestManifest.PROPERTY, manifest.toString());

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> new ApcCoreProvider().getTestCases());

    assertTrue(error.getMessage().contains("Duplicate field"));
  }

  @Test
  void rejectsOversizedManifest() throws Exception {
    Path manifest = writeManifest(" ".repeat(256_001));
    System.setProperty(ApcCustomTestManifest.PROPERTY, manifest.toString());

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> new ApcCoreProvider().getTestCases());

    assertTrue(error.getMessage().contains("manifest exceeds 256000 bytes"));
  }

  @Test
  void positiveCoreCaseDoesNotTreatMatchingFailuresAsParity() throws Exception {
    Connection leftConnection = mock(Connection.class);
    Connection rightConnection = mock(Connection.class);
    Statement leftStatement = mock(Statement.class);
    Statement rightStatement = mock(Statement.class);
    when(leftConnection.createStatement()).thenReturn(leftStatement);
    when(rightConnection.createStatement()).thenReturn(rightStatement);
    when(leftStatement.executeQuery(anyString())).thenThrow(new SQLException("query failed"));

    ApcCoreProvider provider = new ApcCoreProvider();

    assertThrows(
        SQLException.class,
        () ->
            provider.execute(
                leftConnection, rightConnection, provider.getTestCases().get(0), "APC core"));
  }

  private Path writeManifest(String contents) throws Exception {
    Path manifest = tempDir.resolve("accepted-tests.json");
    Files.writeString(manifest, contents);
    return manifest;
  }
}
