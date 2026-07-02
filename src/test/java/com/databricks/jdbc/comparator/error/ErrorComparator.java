package com.databricks.jdbc.comparator.error;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Compares two {@link CapturedOutcome}s field-by-field and produces an {@link ErrorComparison}
 * whose diff strings are kept by {@code ComparisonResult.csvSummary()}:
 *
 * <ul>
 *   <li>One-sided (throw vs result) → a {@code metadataDifferences} entry prefixed with {@link
 *       #ONE_SIDED_PREFIX}, so it survives to the CSV regardless of what the non-throwing side
 *       renders to (a class, a scalar, or {@code null}).
 *   <li>Both threw, field mismatch → {@code dataDifferences} entries of the form {@code "Error
 *       <field> mismatch: <left> vs <right>"} (each contains {@code mismatch}).
 *   <li>Both threw, all fields match → no diffs (MATCH).
 *   <li>Neither threw → NOT_APPLICABLE, no diffs (caller does normal data/metadata comparison).
 * </ul>
 */
public final class ErrorComparator {

  private ErrorComparator() {}

  /**
   * @param describeReturned renders the non-throwing side for one-sided diffs. Receives the
   *     returned value, which may be {@code null} for the NULL outcome, so it must be null-safe.
   */
  public static ErrorComparison compare(
      CapturedOutcome left, CapturedOutcome right, Function<Object, String> describeReturned) {
    List<String> metadataDiffs = new ArrayList<>();
    List<String> dataDiffs = new ArrayList<>();

    boolean leftThrew = left.threw();
    boolean rightThrew = right.threw();

    if (!leftThrew && !rightThrew) {
      return new ErrorComparison(ErrorComparison.Verdict.NOT_APPLICABLE, metadataDiffs, dataDiffs);
    }

    if (leftThrew != rightThrew) {
      metadataDiffs.add(oneSidedDiff(left, right, describeReturned));
      return new ErrorComparison(ErrorComparison.Verdict.ONE_SIDED, metadataDiffs, dataDiffs);
    }

    // Both threw — compare the raw fields. No normalization or format assumptions.
    ErrorFacts l = left.error();
    ErrorFacts r = right.error();

    if (!l.exceptionClass.equals(r.exceptionClass)) {
      dataDiffs.add("Error class mismatch: " + l.simpleClassName() + " vs " + r.simpleClassName());
    }
    if (!equalsNullSafe(l.sqlState, r.sqlState)) {
      dataDiffs.add("Error SQLState mismatch: " + l.sqlState + " vs " + r.sqlState);
    }
    if (l.vendorCode != r.vendorCode) {
      dataDiffs.add("Error code mismatch: " + l.vendorCode + " vs " + r.vendorCode);
    }
    if (!equalsNullSafe(l.message, r.message)) {
      dataDiffs.add("Error message mismatch: '" + l.message + "' vs '" + r.message + "'");
    }

    ErrorComparison.Verdict verdict =
        dataDiffs.isEmpty() ? ErrorComparison.Verdict.MATCH : ErrorComparison.Verdict.MISMATCH;
    return new ErrorComparison(verdict, metadataDiffs, dataDiffs);
  }

  private static boolean equalsNullSafe(Object a, Object b) {
    return a == null ? b == null : a.equals(b);
  }

  /**
   * Stable prefix on one-sided diffs so {@code csvSummary()} always keeps them (regardless of
   * whether the non-throwing side renders to a ResultSet, a scalar, or {@code null}).
   */
  public static final String ONE_SIDED_PREFIX = "Error one-sided: ";

  /**
   * Builds a one-sided diff string carrying {@link #ONE_SIDED_PREFIX}. The thrower is rendered with
   * its extracted facts; the non-throwing side via {@code describeReturned} (which may render to
   * {@code "null"}). Left/right order is preserved so the report shows which side threw.
   */
  private static String oneSidedDiff(
      CapturedOutcome left, CapturedOutcome right, Function<Object, String> describeReturned) {
    String body =
        left.threw()
            ? renderThrower(left.error()) + " vs " + describeReturned.apply(right.value())
            : describeReturned.apply(left.value()) + " vs " + renderThrower(right.error());
    return ONE_SIDED_PREFIX + body;
  }

  private static String renderThrower(ErrorFacts f) {
    return f.exceptionClass + " (SQLSTATE=" + f.sqlState + ", code=" + f.vendorCode + ")";
  }
}
