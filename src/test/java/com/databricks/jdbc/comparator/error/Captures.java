package com.databricks.jdbc.comparator.error;

import com.databricks.jdbc.comparator.ComparisonResult;
import java.util.function.Function;

/**
 * Helper that lets a suite provider capture a JDBC call's outcome (value or thrown error) instead
 * of letting the error propagate and abort the case.
 *
 * <p>Wrap ONLY the single driver call in {@link #capture} — provider bookkeeping (building SQL,
 * opening connections, temp files) must stay outside, so a harness bug still propagates and fails
 * the test loudly rather than being silently captured.
 */
public final class Captures {

  private Captures() {}

  @FunctionalInterface
  public interface JdbcCall {
    Object call() throws Exception;
  }

  /** Runs a driver call, capturing any Throwable as a {@link CapturedOutcome}. */
  public static CapturedOutcome capture(JdbcCall call) {
    try {
      return CapturedOutcome.returned(call.call());
    } catch (Throwable t) {
      return CapturedOutcome.threw(t);
    }
  }

  /**
   * Compares two captured outcomes and folds the result into a {@link ComparisonResult}. Error
   * diffs land in the metadata/data difference lists in the exact formats {@code csvSummary()}
   * understands.
   *
   * @param describeReturned renders the non-throwing side of a one-sided diff (see {@link
   *     ErrorComparator#compare})
   */
  public static ComparisonResult compareCall(
      String queryType,
      String queryOrMethod,
      Object[] args,
      CapturedOutcome left,
      CapturedOutcome right,
      Function<Object, String> describeReturned) {
    ComparisonResult result = new ComparisonResult(queryType, queryOrMethod, args);
    ErrorComparison comparison = ErrorComparator.compare(left, right, describeReturned);
    result.metadataDifferences.addAll(comparison.metadataDiffs);
    result.dataDifferences.addAll(comparison.dataDiffs);
    return result;
  }
}
