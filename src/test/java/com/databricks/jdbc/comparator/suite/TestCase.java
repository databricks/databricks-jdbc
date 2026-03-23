package com.databricks.jdbc.comparator.suite;

import java.util.Objects;

/**
 * Immutable descriptor for a single comparator test case.
 *
 * <p>For SQL suites, {@code identifier} is the query string. For reflection-based suites like
 * DatabaseMetaData, it is the method name. {@code args} holds method arguments (empty for SQL
 * suites).
 */
public final class TestCase {
  private final String identifier;
  private final Object[] args;
  private final String description;

  public TestCase(String identifier, Object[] args, String description) {
    this.identifier = Objects.requireNonNull(identifier, "identifier must not be null");
    this.args = Objects.requireNonNull(args, "args must not be null").clone();
    this.description = Objects.requireNonNull(description, "description must not be null");
  }

  /** Convenience constructor for SQL-based test cases (no args). */
  public TestCase(String query, String description) {
    this(query, new Object[0], description);
  }

  public String getIdentifier() {
    return identifier;
  }

  public Object[] getArgs() {
    return args.clone();
  }

  public String getDescription() {
    return description;
  }

  @Override
  public String toString() {
    return description;
  }
}
