package com.databricks.jdbc.comparator.error;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Bridges suite providers that capture their own {@link CapturedOutcome}s (DML, Volume) to the
 * error comparison, while honoring the {@code ERROR_COMPARISON_MODE} gate the same way {@code
 * ResultSetComparator.compareErrorsDeeply} does — so {@code off} is a true kill switch everywhere.
 *
 * <p>When the gate is on, compares errors deeply (class + SQLState + code + message). When off,
 * falls back to the minimal legacy check (exception class only) so no SQLState/code/message diffs
 * are emitted.
 */
public final class ErrorDiffs {

  private ErrorDiffs() {}

  /**
   * Returns diff strings for a pair of outcomes where at least one threw. Empty when they agree
   * (per the active mode). {@code returnedLabel} prefixes how a returned value is rendered in a
   * one-sided diff (e.g. {@code "update count "}).
   */
  public static List<String> compare(
      CapturedOutcome left, CapturedOutcome right, String returnedLabel) {
    ErrorPolicy policy = ErrorPolicy.active();
    if (!policy.isDeepComparisonEnabled()) {
      return legacyClassOnly(left, right);
    }
    ErrorComparison c = ErrorComparator.compare(left, right, v -> returnedLabel + v);
    List<String> diffs = new ArrayList<>(c.metadataDiffs);
    diffs.addAll(c.dataDiffs);
    return diffs;
  }

  /**
   * Legacy behavior when the gate is off: both threw → flag only a differing exception class;
   * one-sided → flag which side threw. No SQLState/code/message comparison.
   */
  private static List<String> legacyClassOnly(CapturedOutcome left, CapturedOutcome right) {
    if (left.threw() && right.threw()) {
      String l = left.error().simpleClassName();
      String r = right.error().simpleClassName();
      return l.equals(r)
          ? Collections.emptyList()
          : Collections.singletonList("exception type mismatch: " + l + " vs " + r);
    }
    if (left.threw()) {
      return Collections.singletonList(
          "left threw " + left.error().simpleClassName() + " but right returned");
    }
    return Collections.singletonList(
        "right threw " + right.error().simpleClassName() + " but left returned");
  }
}
