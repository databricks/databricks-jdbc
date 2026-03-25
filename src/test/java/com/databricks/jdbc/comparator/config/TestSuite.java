package com.databricks.jdbc.comparator.config;

import com.databricks.jdbc.comparator.suite.*;

/**
 * Identifies a category of comparator tests. Each suite maps to a {@link SuiteProvider} that
 * defines its test cases and execution logic.
 *
 * <p>To add a new suite: create a {@link SuiteProvider} implementation and wire it here.
 */
public enum TestSuite {
  DATABASE_METADATA(null),
  STATEMENT_SELECT(new StatementSelectProvider()),
  STATEMENT_SELECT_TRUNCATED(new StatementSelectTruncatedProvider()),
  STATEMENT_DDL(new StatementDdlProvider()),
  STATEMENT_DML(new StatementDmlProvider()),
  STATEMENT_OTHER(new StatementOtherProvider()),
  PREPARED_STATEMENT_TYPES(new PreparedStatementTypesProvider()),
  PREPARED_STATEMENT_METADATA(new PreparedStatementMetadataProvider()),
  COMPLEX_TYPES(new ComplexTypesProvider()),
  GEOSPATIAL(new GeospatialProvider()),
  NULL_HANDLING(new NullHandlingProvider()),
  VOLUME_OPERATIONS(new VolumeOperationsProvider());

  private final SuiteProvider provider;

  TestSuite(SuiteProvider provider) {
    this.provider = provider;
  }

  /** Returns the provider, or null if the suite is not yet implemented. */
  public SuiteProvider getProvider() {
    return provider;
  }
}
