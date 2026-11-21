package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.common.util.InsertStatementParser;
import com.databricks.jdbc.common.util.SqlCommentParser;
import com.databricks.jdbc.exception.DatabricksBatchUpdateException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
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

    long[] execute(String sql, List<BatchParameterSet> parameterSets) throws SQLException;
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

  long[] executeBatch(List<BatchParameterSet> batchParameterSets) throws SQLException {
    if (batchParameterSets.isEmpty()) {
      return new long[0];
    }
    boolean apc = ApcParameterCapabilities.isApcSea(connection.getSession());
    String sqlForDetection = apc ? SqlCommentParser.stripApcCommentsAndWhitespaces(sql) : sql;
    if (!InsertStatementParser.isParametrizedInsert(sqlForDetection)) {
      return legacyExecutor.executeIndividualStatements(batchParameterSets);
    }
    if (!canUseNativeBatching(batchParameterSets, apc)) {
      return legacyExecutor.executeBatch(batchParameterSets);
    }
    try {
      return nativeExecutor.execute(sql, batchParameterSets);
    } catch (NativeBatchResultException e) {
      throw e;
    } catch (SQLException e) {
      if (isUnsupportedNativeBatching(e)) {
        return legacyExecutor.executeBatch(batchParameterSets);
      }
      long[] failedCounts = new long[batchParameterSets.size()];
      Arrays.fill(failedCounts, Statement.EXECUTE_FAILED);
      throw new DatabricksBatchUpdateException(
          apc ? "APC native batch execution failed: [REDACTED]" : e.getMessage(),
          e.getSQLState(),
          e.getErrorCode(),
          failedCounts,
          apc ? null : e);
    } catch (RuntimeException e) {
      long[] failedCounts = new long[batchParameterSets.size()];
      Arrays.fill(failedCounts, Statement.EXECUTE_FAILED);
      throw new DatabricksBatchUpdateException(
          apc ? "APC native batch execution failed: [REDACTED]" : e.getMessage(),
          DatabricksDriverErrorCode.BATCH_EXECUTE_EXCEPTION.name(),
          DatabricksDriverErrorCode.BATCH_EXECUTE_EXCEPTION.getCode(),
          failedCounts,
          apc ? null : e);
    }
  }

  private boolean isUnsupportedNativeBatching(SQLException exception) {
    return connection.getConnectionContext().getComputeResource() instanceof Warehouse
        && "42P02".equals(exception.getSQLState())
        && exception.getMessage() != null
        && exception.getMessage().contains("[UNBOUND_SQL_PARAMETER]");
  }

  private boolean canUseNativeBatching(List<BatchParameterSet> batchParameterSets, boolean apc) {
    return connection.getConnectionContext().isNativeBatchingEnabled()
        && (apc
                    ? SqlCommentParser.findApcPlaceholderPositions(sql)
                    : SqlCommentParser.findPlaceholderPositions(sql))
                .length
            > 0
        && batchParameterSets.stream().allMatch(set -> !set.isEmpty())
        && (!apc
            || (ApcParameterCapabilities.supportsNativeBatch(connection.getSession())
                // Binary bytes travel as hex under unhex(?), which cannot vary per set.
                && ApcParameterCapabilities.hasConsistentBinaryPositions(batchParameterSets)))
        && nativeExecutor.isSupported();
  }
}
