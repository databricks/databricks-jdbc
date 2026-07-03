package com.databricks.jdbc.comparator.error;

import com.databricks.jdbc.comparator.ComparisonResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Bridges suite providers that capture their own {@link CapturedOutcome}s (DML, Volume, and the
 * connection-state/transaction suites) to the error comparison, while honoring the {@code
 * ERROR_COMPARISON_MODE} gate the same way {@code ResultSetComparator.compareErrorsDeeply} does —
 * so {@code off} is a true kill switch everywhere.
 *
 * <p>When the gate is on, compares errors deeply (class + SQLState + code + message). When off,
 * falls back to the minimal legacy check (exception class only) so no SQLState/code/message diffs
 * are emitted.
 */
public final class ErrorDiffs {

  private ErrorDiffs() {}

  /**
   * Compares two captured outcomes and folds the diffs into {@code result}, preserving the
   * metadata/data split that {@code ComparisonResult.csvSummary()} relies on — one-sided diffs
   * (carrying {@link ErrorComparator#ONE_SIDED_PREFIX}) go to {@code metadataDifferences}, field
   * mismatches to {@code dataDifferences}. Callers should prefer this over {@link #compare} so a
   * one-sided error is not mis-filed and dropped from the CSV summary. Honors the gate:
   * neither-threw and off-mode legacy behavior match {@link #compare}. {@code prefix} (may be
   * empty) is prepended to each diff for readable per-operation reports.
   */
  public static void foldInto(
      ComparisonResult result,
      CapturedOutcome left,
      CapturedOutcome right,
      String returnedLabel,
      String prefix) {
    if (!left.threw() && !right.threw()) {
      return;
    }
    ErrorPolicy policy = ErrorPolicy.active();
    if (!policy.isDeepComparisonEnabled()) {
      for (String d : legacyClassOnly(left, right)) {
        result.dataDifferences.add(prefix + d);
      }
      return;
    }
    ErrorComparison c = ErrorComparator.compare(left, right, v -> returnedLabel + v);
    for (String d : c.metadataDiffs) {
      result.metadataDifferences.add(prefix + d);
    }
    for (String d : c.dataDiffs) {
      result.dataDifferences.add(prefix + d);
    }
  }

  /**
   * Returns a FLAT list of diff strings for a pair of outcomes where at least one threw (empty when
   * they agree, per the active mode). {@code returnedLabel} prefixes how a returned value is
   * rendered in a one-sided diff (e.g. {@code "update count "}).
   *
   * <p>NOTE: this flattens metadata and data diffs together, so a caller that dumps the result into
   * {@code ComparisonResult.dataDifferences} will mis-file one-sided diffs (which belong in {@code
   * metadataDifferences}) and drop them from {@code csvSummary()}. Prefer {@link #foldInto}, which
   * preserves the split. Retained for callers that accumulate into their own flat list (the DML and
   * Volume positive suites).
   */
  public static List<String> compare(
      CapturedOutcome left, CapturedOutcome right, String returnedLabel) {
    // Neither side threw — nothing to compare here (mirrors the deep path's NOT_APPLICABLE).
    // Guard centrally so every caller is safe, including callers that invoke compare()
    // unconditionally (the negative DDL/DML/Batch suites); their both-succeed cases (e.g.
    // DROP TABLE IF EXISTS) must not dereference the null error() in off mode.
    if (!left.threw() && !right.threw()) {
      return Collections.emptyList();
    }
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
