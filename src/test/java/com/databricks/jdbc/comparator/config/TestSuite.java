package com.databricks.jdbc.comparator.config;

import com.databricks.jdbc.comparator.suite.StatementSelectProvider;
import com.databricks.jdbc.comparator.suite.SuiteProvider;

/**
 * Identifies a category of comparator tests. Each suite maps to a {@link SuiteProvider} that
 * defines its test cases and execution logic.
 *
 * <p>To add a new suite: create a {@link SuiteProvider} implementation and wire it here.
 */
public enum TestSuite {
  DATABASE_METADATA(null),
  STATEMENT_SELECT(new StatementSelectProvider()),
  STATEMENT_SELECT_TRUNCATED(null),
  STATEMENT_DDL(null),
  STATEMENT_DML(null),
  STATEMENT_OTHER(null),
  PREPARED_STATEMENT_TYPES(null),
  PREPARED_STATEMENT_METADATA(null),
  COMPLEX_TYPES(null),
  GEOSPATIAL(null),
  NULL_HANDLING(null),
  VOLUME_OPERATIONS(null);

  private final SuiteProvider provider;

  TestSuite(SuiteProvider provider) {
    this.provider = provider;
  }

  /** Returns the provider, or null if the suite is not yet implemented. */
  public SuiteProvider getProvider() {
    return provider;
  }
}
