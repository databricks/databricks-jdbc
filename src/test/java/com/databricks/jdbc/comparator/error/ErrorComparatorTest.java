package com.databricks.jdbc.comparator.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.comparator.ComparisonResult;
import java.sql.SQLException;
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
  void unrecognizedModeDefaultsToOffInsteadOfThrowing() {
    String prev = System.getProperty("ERROR_COMPARISON_MODE");
    try {
      System.setProperty("ERROR_COMPARISON_MODE", "shaddow"); // typo
      ErrorPolicy p = ErrorPolicy.active();
      assertEquals(ErrorPolicy.Mode.OFF, p.mode());
      assertFalse(p.isDeepComparisonEnabled());
    } finally {
      if (prev == null) {
        System.clearProperty("ERROR_COMPARISON_MODE");
      } else {
        System.setProperty("ERROR_COMPARISON_MODE", prev);
      }
    }
  }
}
