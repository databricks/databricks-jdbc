package com.databricks.jdbc.comparator.config;

import com.databricks.jdbc.comparator.suite.*;

/**
 * Identifies a category of comparator tests. Each suite maps to a {@link SuiteProvider} that
 * defines its test cases and execution logic.
 *
 * <p>To add a new suite: create a {@link SuiteProvider} implementation and wire it here.
 */
public enum TestSuite {
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
  VOLUME_OPERATIONS(new VolumeOperationsProvider()),
  DATABASE_METADATA(new DatabaseMetaDataProvider()),

  // Negative (error-provoking) suites — read-only, run on the shared connections.
  NEGATIVE_STATEMENT_SELECT(new NegativeStatementSelectProvider()),
  NEGATIVE_STATEMENT_OTHER(new NegativeStatementOtherProvider()),
  NEGATIVE_PARAM_BINDING(new NegativeParamBindingProvider()),
  NEGATIVE_PREPARED_METADATA(new NegativePreparedMetadataProvider()),
  NEGATIVE_TYPE_CONVERSION(new NegativeTypeConversionProvider()),

  // Negative suites that mutate catalog objects — isolated in their own namespace
  // under comparator_ddl_tests (seeded fresh and dropped), so safe on the shared connections.
  NEGATIVE_STATEMENT_DDL(new NegativeStatementDdlProvider()),
  NEGATIVE_STATEMENT_DML(new NegativeStatementDmlProvider()),
  NEGATIVE_STATEMENT_BATCH(new NegativeStatementBatchProvider()),

  // Negative suites that mutate connection/session state — each opens its OWN fresh connections
  // via ConnectionFactory (closed in a finally), so they never poison the shared connections.
  NEGATIVE_CONNECTION_STATE(new NegativeConnectionStateProvider()),
  NEGATIVE_TRANSACTION(new NegativeTransactionProvider());

  private final SuiteProvider provider;

  TestSuite(SuiteProvider provider) {
    this.provider = provider;
  }

  /** Returns the provider, or null if the suite is not yet implemented. */
  public SuiteProvider getProvider() {
    return provider;
  }
}
