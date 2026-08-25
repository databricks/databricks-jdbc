package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.exception.DatabricksBatchUpdateException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

class PreparedStatementBatchExecutor {

  private final LegacyPreparedStatementBatchExecutor legacyExecutor;

  @FunctionalInterface
  interface StatementExecutor {
    DatabricksResultSet execute(
        String sql,
        Map<Integer, ImmutableSqlParameter> params,
        StatementType statementType,
        boolean closeStatement)
        throws SQLException;
  }

  PreparedStatementBatchExecutor(
      String sql,
      DatabricksConnection connection,
      boolean interpolateParameters,
      StatementExecutor statementExecutor) {
    this.legacyExecutor =
        new LegacyPreparedStatementBatchExecutor(
            sql, connection, interpolateParameters, statementExecutor);
  }

  long[] executeBatch(List<DatabricksParameterMetaData> batchParameterMetaData)
      throws DatabricksBatchUpdateException {
    return legacyExecutor.executeBatch(batchParameterMetaData);
  }
}
