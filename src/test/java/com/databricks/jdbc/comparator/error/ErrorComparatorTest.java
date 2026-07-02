package com.databricks.jdbc.comparator.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.comparator.ComparisonResult;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for the pure error capture/compare logic (no JDBC connection required). */
public class ErrorComparatorTest {

  // ---- ErrorFacts extraction ----

  @Test
  void extractsSqlStateAndVendorCodeFromSqlException() {
    ErrorFacts f = ErrorFacts.from(new SQLException("boom", "42P01", 500593));
    assertEquals("java.sql.SQLException", f.exceptionClass);
    assertEquals("42P01", f.sqlState);
    assertEquals(500593, f.vendorCode);
    assertEquals("boom", f.message);
  }

  @Test
  void nonSqlThrowableHasNullStateAndZeroCode() {
    ErrorFacts f = ErrorFacts.from(new RuntimeException("plain"));
    assertNull(f.sqlState);
    assertEquals(0, f.vendorCode);
  }

  @Test
  void nullMessageIsNpeSafe() {
    ErrorFacts f = ErrorFacts.from(new RuntimeException((String) null));
    assertNull(f.message);
  }

  // ---- Verdicts ----

  @Test
  void bothReturnedIsNotApplicable() {
    ErrorComparison c =
        ErrorComparator.compare(
            CapturedOutcome.returned("a"), CapturedOutcome.returned("b"), o -> "class RS");
    assertEquals(ErrorComparison.Verdict.NOT_APPLICABLE, c.verdict);
    assertFalse(c.hasDiffs());
  }

  @Test
  void identicalErrorsMatch() {
    CapturedOutcome l = CapturedOutcome.threw(new SQLException("not found", "42P01", 1));
    CapturedOutcome r = CapturedOutcome.threw(new SQLException("not found", "42P01", 1));
    ErrorComparison c = ErrorComparator.compare(l, r, o -> "class RS");
    assertEquals(ErrorComparison.Verdict.MATCH, c.verdict);
    assertFalse(c.hasDiffs());
  }

  @Test
  void differingSqlStateIsMismatch() {
    CapturedOutcome l = CapturedOutcome.threw(new SQLException("x", "42P01", 1));
    CapturedOutcome r = CapturedOutcome.threw(new SQLException("x", "08000", 1));
    ErrorComparison c = ErrorComparator.compare(l, r, o -> "class RS");
    assertEquals(ErrorComparison.Verdict.MISMATCH, c.verdict);
    assertTrue(
        c.dataDiffs.stream().anyMatch(d -> d.equals("Error SQLState mismatch: 42P01 vs 08000")));
  }

  @Test
  void differingMessageIsMismatch() {
    CapturedOutcome l = CapturedOutcome.threw(new SQLException("left message", "42P01", 1));
    CapturedOutcome r = CapturedOutcome.threw(new SQLException("right message", "42P01", 1));
    ErrorComparison c = ErrorComparator.compare(l, r, o -> "class RS");
    assertEquals(ErrorComparison.Verdict.MISMATCH, c.verdict);
    assertTrue(c.dataDiffs.stream().anyMatch(d -> d.startsWith("Error message mismatch:")));
  }

  @Test
  void oneSidedCarriesStablePrefixThrowerLeft() {
    CapturedOutcome thrower = CapturedOutcome.threw(new SQLException("boom", "42P01", 0));
    CapturedOutcome returned = CapturedOutcome.returned("rs");
    ErrorComparison c =
        ErrorComparator.compare(thrower, returned, o -> "class com.example.RS (5 rows)");
    assertEquals(ErrorComparison.Verdict.ONE_SIDED, c.verdict);
    assertEquals(1, c.metadataDiffs.size());
    assertTrue(c.metadataDiffs.get(0).startsWith(ErrorComparator.ONE_SIDED_PREFIX));
  }

  @Test
  void oneSidedCarriesStablePrefixThrowerRight() {
    CapturedOutcome returned = CapturedOutcome.returned("rs");
    CapturedOutcome thrower = CapturedOutcome.threw(new SQLException("boom", "42P01", 0));
    ErrorComparison c =
        ErrorComparator.compare(returned, thrower, o -> "class com.example.RS (5 rows)");
    assertEquals(ErrorComparison.Verdict.ONE_SIDED, c.verdict);
    assertEquals(1, c.metadataDiffs.size());
    assertTrue(c.metadataDiffs.get(0).startsWith(ErrorComparator.ONE_SIDED_PREFIX));
  }

  @Test
  void oneSidedThrowVsNullDoesNotThrowAndCarriesPrefix() {
    // One side throws, the other returns null -> describer receives null. Must not NPE, and the
    // diff must carry the stable prefix (the " vs class " marker does NOT survive here).
    CapturedOutcome thrower = CapturedOutcome.threw(new SQLException("boom", "42P01", 0));
    CapturedOutcome nullSide = CapturedOutcome.returned(null);
    ErrorComparison c =
        ErrorComparator.compare(thrower, nullSide, o -> o == null ? "null" : "class " + o);
    assertEquals(ErrorComparison.Verdict.ONE_SIDED, c.verdict);
    assertEquals(1, c.metadataDiffs.size());
    assertTrue(c.metadataDiffs.get(0).startsWith(ErrorComparator.ONE_SIDED_PREFIX));
    assertTrue(c.metadataDiffs.get(0).endsWith(" vs null"));
  }

  @Test
  void csvSummaryKeepsOneSidedThrowVsNull() {
    // The regression from review: a throw-vs-null one-sided diff must survive into csvSummary().
    CapturedOutcome thrower = CapturedOutcome.threw(new SQLException("boom", "42P01", 0));
    CapturedOutcome nullSide = CapturedOutcome.returned(null);
    ErrorComparison c =
        ErrorComparator.compare(thrower, nullSide, o -> o == null ? "null" : "class " + o);
    ComparisonResult result = new ComparisonResult("t", "q", new Object[0]);
    result.metadataDifferences.addAll(c.metadataDiffs);
    result.dataDifferences.addAll(c.dataDiffs);
    assertTrue(result.hasDifferences());
    assertFalse(
        result.csvSummary().isEmpty(), "one-sided throw-vs-null must appear in CSV summary");
    assertTrue(result.csvSummary().contains(ErrorComparator.ONE_SIDED_PREFIX));
  }

  @Test
  void offModeDisablesDeepComparison() {
    assertFalse(ErrorPolicy.of(ErrorPolicy.Mode.OFF).isDeepComparisonEnabled());
    assertTrue(ErrorPolicy.of(ErrorPolicy.Mode.SHADOW).isDeepComparisonEnabled());
  }

  @Test
  void unrecognizedModeDefaultsToShadowInsteadOfThrowing() {
    String prev = System.getProperty("ERROR_COMPARISON_MODE");
    try {
      System.setProperty("ERROR_COMPARISON_MODE", "shaddow"); // typo
      ErrorPolicy p = ErrorPolicy.active();
      // Fail safe: a bad value must not throw. It falls back to the default (shadow), which is
      // itself CI-safe (records DIFF rows, never fails the build).
      assertEquals(ErrorPolicy.Mode.SHADOW, p.mode());
      assertTrue(p.isDeepComparisonEnabled());
    } finally {
      if (prev == null) {
        System.clearProperty("ERROR_COMPARISON_MODE");
      } else {
        System.setProperty("ERROR_COMPARISON_MODE", prev);
      }
    }
  }

  @Test
  void unsetModeDefaultsToShadow() {
    String prev = System.getProperty("ERROR_COMPARISON_MODE");
    try {
      System.clearProperty("ERROR_COMPARISON_MODE");
      assertEquals(ErrorPolicy.Mode.SHADOW, ErrorPolicy.active().mode());
    } finally {
      if (prev != null) {
        System.setProperty("ERROR_COMPARISON_MODE", prev);
      }
    }
  }

  // ---- ErrorDiffs gate (used by DML/Volume providers) ----

  @Test
  void errorDiffsOffModeDoesLegacyClassOnly() {
    withMode(
        "off",
        () -> {
          // Same class, different SQLState/message -> OFF must NOT emit a SQLState/message diff.
          CapturedOutcome l = CapturedOutcome.threw(new SQLException("a", "42P01", 1));
          CapturedOutcome r = CapturedOutcome.threw(new SQLException("b", "08000", 2));
          assertTrue(ErrorDiffs.compare(l, r, "v ").isEmpty());
        });
  }

  @Test
  void errorDiffsShadowModeComparesDeeply() {
    withMode(
        "shadow",
        () -> {
          CapturedOutcome l = CapturedOutcome.threw(new SQLException("a", "42P01", 1));
          CapturedOutcome r = CapturedOutcome.threw(new SQLException("b", "08000", 2));
          List<String> diffs = ErrorDiffs.compare(l, r, "v ");
          assertTrue(diffs.stream().anyMatch(d -> d.contains("SQLState mismatch")));
        });
  }

  @Test
  void errorDiffsOffModeStillFlagsDifferingClass() {
    withMode(
        "off",
        () -> {
          CapturedOutcome l = CapturedOutcome.threw(new SQLException("a", "42P01", 1));
          CapturedOutcome r = CapturedOutcome.threw(new java.sql.SQLDataException("b", "42P01", 1));
          List<String> diffs = ErrorDiffs.compare(l, r, "v ");
          assertTrue(diffs.stream().anyMatch(d -> d.contains("exception type mismatch")));
        });
  }

  private static void withMode(String mode, Runnable body) {
    String prev = System.getProperty("ERROR_COMPARISON_MODE");
    try {
      System.setProperty("ERROR_COMPARISON_MODE", mode);
      body.run();
    } finally {
      if (prev == null) {
        System.clearProperty("ERROR_COMPARISON_MODE");
      } else {
        System.setProperty("ERROR_COMPARISON_MODE", prev);
      }
    }
  }
}
