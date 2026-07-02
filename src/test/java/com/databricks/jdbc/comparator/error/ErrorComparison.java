package com.databricks.jdbc.comparator.error;

import java.util.List;

/**
 * The outcome of comparing two {@link CapturedOutcome}s: a {@link Verdict} plus the diff strings to
 * fold into a {@link com.databricks.jdbc.comparator.ComparisonResult}.
 *
 * <p>Diff strings are pre-formatted in the exact shapes {@code ComparisonResult.csvSummary()}
 * understands (see {@link ErrorComparator}), so callers just append them to the appropriate
 * differences list.
 */
public final class ErrorComparison {

  public enum Verdict {
    /** Both threw and every compared field matched (or was tolerated). */
    MATCH,
    /** Both threw but at least one compared field differed. */
    MISMATCH,
    /** One side threw, the other returned/returned-null. */
    ONE_SIDED,
    /** Neither side threw — fall back to normal data/metadata comparison. */
    NOT_APPLICABLE
  }

  public final Verdict verdict;

  /** Diffs destined for {@code metadataDifferences} (one-sided "... vs class ..." strings). */
  public final List<String> metadataDiffs;

  /** Diffs destined for {@code dataDifferences} ("Error <field> mismatch: ..." strings). */
  public final List<String> dataDiffs;

  public ErrorComparison(Verdict verdict, List<String> metadataDiffs, List<String> dataDiffs) {
    this.verdict = verdict;
    this.metadataDiffs = metadataDiffs;
    this.dataDiffs = dataDiffs;
  }

  public boolean hasDiffs() {
    return !metadataDiffs.isEmpty() || !dataDiffs.isEmpty();
  }
}
