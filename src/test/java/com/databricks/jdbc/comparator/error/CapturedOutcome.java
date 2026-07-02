package com.databricks.jdbc.comparator.error;

/**
 * What one side of a comparison did for a single JDBC call: it {@link Kind#THREW threw}, {@link
 * Kind#RETURNED returned} a value, or returned {@link Kind#NULL null}.
 *
 * <p>This lets a thrown error be treated as a first-class, comparable result rather than
 * propagating and aborting the case. Capture a call with {@link Captures#capture}.
 */
public final class CapturedOutcome {

  public enum Kind {
    THREW,
    RETURNED,
    NULL
  }

  private final Kind kind;
  private final Object value; // non-null only when RETURNED (ResultSet, update count, Boolean, ...)
  private final Throwable throwable; // non-null only when THREW
  private final ErrorFacts error; // non-null only when THREW

  private CapturedOutcome(Kind kind, Object value, Throwable throwable, ErrorFacts error) {
    this.kind = kind;
    this.value = value;
    this.throwable = throwable;
    this.error = error;
  }

  public static CapturedOutcome threw(Throwable t) {
    return new CapturedOutcome(Kind.THREW, null, t, ErrorFacts.from(t));
  }

  public static CapturedOutcome returned(Object value) {
    return value == null ? nul() : new CapturedOutcome(Kind.RETURNED, value, null, null);
  }

  public static CapturedOutcome nul() {
    return new CapturedOutcome(Kind.NULL, null, null, null);
  }

  public Kind kind() {
    return kind;
  }

  public boolean threw() {
    return kind == Kind.THREW;
  }

  /** The returned value (may be null); meaningful only when {@link #kind()} is RETURNED. */
  public Object value() {
    return value;
  }

  /** The raw Throwable; non-null only when {@link #threw()} is true. */
  public Throwable throwable() {
    return throwable;
  }

  /** The extracted error fields; non-null only when {@link #threw()} is true. */
  public ErrorFacts error() {
    return error;
  }
}
