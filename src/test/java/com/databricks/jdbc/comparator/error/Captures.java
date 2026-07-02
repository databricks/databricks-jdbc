package com.databricks.jdbc.comparator.error;

import com.databricks.jdbc.comparator.ComparisonResult;
import java.sql.ResultSet;
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
   * Runs a driver call and returns its result on success, or the thrown {@link Throwable} itself on
   * failure — so the value can be handed directly to {@link
   * com.databricks.jdbc.comparator.ResultSetComparator#compare}, which dispatches on runtime type
   * (ResultSet vs Throwable) and, when the error gate is on, compares thrown errors deeply.
   *
   * <p>Wrap ONLY the driver call; keep provider bookkeeping outside so harness bugs still
   * propagate.
   */
  public static Object resultOrThrowable(JdbcCall call) {
    try {
      return call.call();
    } catch (Throwable t) {
      return t;
    }
  }

  /** Closes the value if it is a ResultSet; ignores nulls, non-ResultSets, and close errors. */
  public static void closeIfResultSet(Object value) {
    if (value instanceof ResultSet) {
      try {
        ((ResultSet) value).close();
      } catch (Exception ignored) {
        // best-effort cleanup
      }
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
