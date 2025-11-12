package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.InsertStatementParser;
import com.databricks.jdbc.exception.DatabricksBatchUpdateException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class PreparedStatementBatchExecutor {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(PreparedStatementBatchExecutor.class);

  private final String sql;
  private final DatabricksConnection connection;
  private final boolean interpolateParameters;
  private final StatementExecutor statementExecutor;

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
    this.sql = sql;
    this.connection = connection;
    this.interpolateParameters = interpolateParameters;
    this.statementExecutor = statementExecutor;
  }

  long[] executeBatch(List<DatabricksParameterMetaData> batchParameterMetaData)
      throws DatabricksBatchUpdateException {
    if (batchParameterMetaData.isEmpty()) {
      return new long[0];
    }

    // Try to optimize INSERT statements with multi-row batching
    if (canUseBatchedInsert()) {
      return executeBatchedInsert(batchParameterMetaData);
    } else {
      // Fall back to individual execution for non-INSERT or incompatible statements
      return executeIndividualStatements(batchParameterMetaData);
    }
  }

  private boolean canUseBatchedInsert() {
    // Check if batched inserts are enabled via connection property
    if (!connection.getConnectionContext().isBatchedInsertsEnabled()) {
      return false;
    }

    // Use strict exception-based parsing for better error handling
    try {
      InsertStatementParser.parseInsertStrict(sql);
      return true;
    } catch (Exception e) {
      // Not a valid INSERT statement suitable for batching
      return false;
    }
  }

  private long[] executeBatchedInsert(List<DatabricksParameterMetaData> batchParameterMetaData)
      throws DatabricksBatchUpdateException {
    LOGGER.debug("Executing batched INSERT with {} rows", batchParameterMetaData.size());

    try {
      InsertStatementParser.InsertInfo insertInfo = InsertStatementParser.parseInsertStrict(sql);

      // Calculate how many rows we can fit in one chunk
      int parametersPerRow = insertInfo.getColumnCount();
      int maxRowsPerChunk;

      if (interpolateParameters) {
        // When parameter interpolation is enabled (supportManyParameters=1), there is no
        // parameter limit since values are interpolated directly into the SQL string.
        // Try to execute all rows in a single batch, only limited by configured BatchInsertSize
        // which users can set based on their data to avoid exceeding the 16MB statement limit.
        int configuredBatchSize = connection.getConnectionContext().getBatchInsertSize();
        if (configuredBatchSize < 1) {
          throw new DatabricksSQLException(
              "BatchInsertSize must be at least 1, got: " + configuredBatchSize,
              DatabricksDriverErrorCode.INVALID_STATE);
        }
        maxRowsPerChunk = Math.min(configuredBatchSize, batchParameterMetaData.size());
      } else {
        // When using parameterized queries, respect the 256 parameter limit from Databricks
        // backend
        int maxRowsByParameterLimit =
            DatabricksJdbcConstants.MAX_QUERY_PARAMETERS / parametersPerRow;

        // Ensure we have at least 1 row per chunk
        if (maxRowsByParameterLimit < 1) {
          maxRowsPerChunk = 1;
        } else {
          maxRowsPerChunk = maxRowsByParameterLimit;
        }
      }

      long[] allUpdateCounts = new long[batchParameterMetaData.size()];

      // Process batches in chunks
      for (int startIndex = 0;
          startIndex < batchParameterMetaData.size();
          startIndex += maxRowsPerChunk) {
        int endIndex = Math.min(startIndex + maxRowsPerChunk, batchParameterMetaData.size());
        int chunkSize = endIndex - startIndex;

        // Build multi-row INSERT for this chunk
        String multiRowSql = InsertStatementParser.generateMultiRowInsert(insertInfo, chunkSize);
        Map<Integer, ImmutableSqlParameter> chunkParams = new HashMap<>();
        int paramIndex = 1;

        for (int i = startIndex; i < endIndex; i++) {
          DatabricksParameterMetaData batchParams = batchParameterMetaData.get(i);
          Map<Integer, ImmutableSqlParameter> rowParams = batchParams.getParameterBindings();
          for (int j = 1; j <= rowParams.size(); j++) {
            if (rowParams.containsKey(j)) {
              chunkParams.put(paramIndex++, rowParams.get(j));
            }
          }
        }

        // Execute this chunk
        String sqlToExecute =
            interpolateParameters
                ? com.databricks.jdbc.common.util.SQLInterpolator.interpolateSQL(
                    multiRowSql, chunkParams)
                : multiRowSql;
        Map<Integer, ImmutableSqlParameter> paramsToSend =
            interpolateParameters ? new HashMap<>() : chunkParams;
        statementExecutor.execute(sqlToExecute, paramsToSend, StatementType.UPDATE, false);

        // Set update counts for this chunk (each row typically affects 1 row)
        for (int i = startIndex; i < endIndex; i++) {
          allUpdateCounts[i] = 1;
        }
      }

      return allUpdateCounts;

    } catch (DatabricksBatchUpdateException e) {
      // Re-throw batch update exceptions (these already have proper update counts)
      throw e;
    } catch (Exception e) {
      // Unexpected exception - mark all as failed
      LOGGER.error("Unexpected error executing batched INSERT: {}", e.getMessage(), e);
      long[] failedCounts = new long[batchParameterMetaData.size()];
      for (int i = 0; i < failedCounts.length; i++) {
        failedCounts[i] = Statement.EXECUTE_FAILED;
      }
      throw new DatabricksBatchUpdateException(
          e.getMessage(), DatabricksDriverErrorCode.BATCH_EXECUTE_EXCEPTION, failedCounts);
    }
  }

  private long[] executeIndividualStatements(
      List<DatabricksParameterMetaData> batchParameterMetaData)
      throws DatabricksBatchUpdateException {
    LOGGER.debug("Executing batch individually with {} statements", batchParameterMetaData.size());
    long[] largeUpdateCount = new long[batchParameterMetaData.size()];

    for (int sqlQueryIndex = 0; sqlQueryIndex < batchParameterMetaData.size(); sqlQueryIndex++) {
      DatabricksParameterMetaData databricksParameterMetaData =
          batchParameterMetaData.get(sqlQueryIndex);
      try {
        DatabricksResultSet resultSet =
            statementExecutor.execute(
                sql,
                databricksParameterMetaData.getParameterBindings(),
                StatementType.UPDATE,
                false);
        largeUpdateCount[sqlQueryIndex] = resultSet.getUpdateCount();
      } catch (Exception e) {
        LOGGER.error(
            "Error executing batch update for index {}: {}", sqlQueryIndex, e.getMessage(), e);
        // Set the current failed statement's count
        largeUpdateCount[sqlQueryIndex] = Statement.EXECUTE_FAILED;
        // Set all remaining statements as failed
        for (int i = sqlQueryIndex + 1; i < largeUpdateCount.length; i++) {
          largeUpdateCount[i] = Statement.EXECUTE_FAILED;
        }
        // WARNING: Due to lack of transaction support, any successfully executed statements
        // before this failure have already been committed and cannot be rolled back
        throw new DatabricksBatchUpdateException(
            e.getMessage(), DatabricksDriverErrorCode.BATCH_EXECUTE_EXCEPTION, largeUpdateCount);
      }
    }
    return largeUpdateCount;
  }
}
