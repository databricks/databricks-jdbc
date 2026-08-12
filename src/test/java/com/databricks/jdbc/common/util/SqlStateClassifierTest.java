package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.util.SqlStateClassifier.classifyTransientSqlState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class SqlStateClassifierTest {

  @Test
  void unityCatalogTokenRemapsTo08S01() {
    String ucMessage =
        "Error running query: [UC_CLIENT_EXCEPTION] "
            + "com.databricks.sql.managedcatalog.UnityCatalogClientException: "
            + "[UC_CLIENT_EXCEPTION] Failed to contact the Unity Catalog server. "
            + "HTTP/1.1 504 Gateway Timeout, DEADLINE_EXCEEDED";
    assertEquals("08S01", classifyTransientSqlState(ucMessage, "XXUCC"));
  }

  @Test
  void parquetReadFooterTokenRemapsTo08S01() {
    String message =
        "Error running query: [PARQUET_FAILED_READ_FOOTER] "
            + "com.databricks.sql.io.parquet.ParquetFailedReadFooterException: "
            + "DEADLINE_EXCEEDED: acquiring connection";
    assertEquals("08S01", classifyTransientSqlState(message, null));
  }

  @Test
  void deadlineExceededAcquiringConnectionRemapsTo08S01() {
    assertEquals(
        "08S01",
        classifyTransientSqlState("DEADLINE_EXCEEDED: acquiring connection from pool", null));
  }

  @Test
  void concurrentModificationGatedOn42000() {
    String message =
        "Error running query: java.util.ConcurrentModificationException: "
            + "mutation occurred during iteration";
    assertEquals("40001", classifyTransientSqlState(message, "42000"));
  }

  @Test
  void concurrentModificationDoesNotRemapWhenOriginalStateIsNot42000() {
    String message =
        "Error running query: java.util.ConcurrentModificationException: "
            + "mutation occurred during iteration";
    assertEquals("42501", classifyTransientSqlState(message, "42501"));
    assertEquals("XXUCC", classifyTransientSqlState(message, "XXUCC"));
    assertNull(classifyTransientSqlState(message, null));
  }

  @Test
  void bareConcurrentModificationExceptionWithoutFqnDoesNotRemap() {
    assertEquals(
        "42000",
        classifyTransientSqlState(
            "SELECT 'ConcurrentModificationException' FROM nonexistent_tbl", "42000"));
  }

  @Test
  void ucProseWithoutTokenDoesNotTriggerRemap() {
    assertEquals(
        "42S02",
        classifyTransientSqlState(
            "User-supplied literal: Failed to contact the Unity Catalog server", "42S02"));
  }

  @Test
  void unrelatedErrorPreservesOriginalState() {
    assertEquals(
        "42S02", classifyTransientSqlState("Table or view not found: foo.bar.baz", "42S02"));
  }

  @Test
  void nullMessagePreservesOriginalState() {
    assertNull(classifyTransientSqlState(null, null));
    assertEquals("42S02", classifyTransientSqlState(null, "42S02"));
  }

  @Test
  void emptyOriginalStateIsPreservedWhenUnrelated() {
    assertEquals("", classifyTransientSqlState("Some unrelated error", ""));
  }

  @Test
  void caseSensitivityIsExplicit() {
    assertEquals(
        "42S02",
        classifyTransientSqlState("Lowercase [uc_client_exception] should not match", "42S02"));
  }

  @Test
  void ucCheckRunsBeforeCmeWhenOriginalStateIs42000AndBothSubstringsPresent() {
    String message =
        "[UC_CLIENT_EXCEPTION] catalog server: caused by "
            + "java.util.ConcurrentModificationException: nested";
    assertEquals("08S01", classifyTransientSqlState(message, "42000"));
  }
}
