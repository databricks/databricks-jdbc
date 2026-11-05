package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.InsertStatementParser;
import com.databricks.jdbc.exception.DatabricksBatchUpdateException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

class PreparedStatementBatchExecutor {

  private static final NativeBatchExecutor UNSUPPORTED_NATIVE_EXECUTOR =
      new NativeBatchExecutor() {
        @Override
        public boolean isSupported() {
          return false;
        }

        @Override
        public long[] execute(String sql, List<BatchParameterSet> parameterSets) {
          throw new IllegalStateException("Native batch execution is not supported");
        }
      };

  private final String sql;
  private final DatabricksConnection connection;
  private final LegacyPreparedStatementBatchExecutor legacyExecutor;
  private final NativeBatchExecutor nativeExecutor;

  @FunctionalInterface
  interface StatementExecutor {
    DatabricksResultSet execute(
        String sql,
        Map<Integer, ImmutableSqlParameter> params,
        StatementType statementType,
        boolean closeStatement)
        throws SQLException;
  }

  interface NativeBatchExecutor {
    boolean isSupported();

    long[] execute(String sql, List<BatchParameterSet> parameterSets)
        throws DatabricksBatchUpdateException;
  }

  PreparedStatementBatchExecutor(
      String sql,
      DatabricksConnection connection,
      boolean interpolateParameters,
      StatementExecutor statementExecutor) {
    this(sql, connection, interpolateParameters, statementExecutor, UNSUPPORTED_NATIVE_EXECUTOR);
  }

  PreparedStatementBatchExecutor(
      String sql,
      DatabricksConnection connection,
      boolean interpolateParameters,
      StatementExecutor statementExecutor,
      NativeBatchExecutor nativeExecutor) {
    this.sql = sql;
    this.connection = connection;
    this.legacyExecutor =
        new LegacyPreparedStatementBatchExecutor(
            sql, connection, interpolateParameters, statementExecutor);
    this.nativeExecutor = nativeExecutor;
  }

  long[] executeBatch(List<BatchParameterSet> batchParameterSets)
      throws DatabricksBatchUpdateException {
    if (canUseNativeBatching(batchParameterSets)) {
      return nativeExecutor.execute(sql, batchParameterSets);
    }
    return legacyExecutor.executeBatch(batchParameterSets);
  }

  private boolean canUseNativeBatching(List<BatchParameterSet> batchParameterSets) {
    return !batchParameterSets.isEmpty()
        && connection.getConnectionContext().isNativeBatchingEnabled()
        && InsertStatementParser.isParametrizedInsert(sql)
        && nativeExecutor.isSupported();
  }
}
