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
      int processedRows = 0;

      // Process batches in chunks with per-chunk retry logic
      for (int startIndex = 0;
          startIndex < batchParameterMetaData.size();
          startIndex += maxRowsPerChunk) {
        int endIndex = Math.min(startIndex + maxRowsPerChunk, batchParameterMetaData.size());

        // Execute chunk with retry and split logic (starting at depth 0)
        processedRows +=
            executeChunkWithRetryAndSplit(
                insertInfo,
                batchParameterMetaData,
                startIndex,
                endIndex,
                allUpdateCounts,
                interpolateParameters,
                0);
      }

      LOGGER.debug("Successfully processed {} rows in chunks", processedRows);
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

  private int executeChunkWithRetryAndSplit(
      InsertStatementParser.InsertInfo insertInfo,
      List<DatabricksParameterMetaData> batchParameterMetaData,
      int startIndex,
      int endIndex,
      long[] allUpdateCounts,
      boolean interpolateParameters,
      int depth)
      throws DatabricksBatchUpdateException {

    int chunkSize = endIndex - startIndex;
    LOGGER.debug(
        "Processing chunk: rows {}-{} (inclusive) - {} rows at depth {}",
        startIndex,
        endIndex - 1,
        chunkSize,
        depth);

    // Prepare SQL and parameters once (not on every retry)
    String multiRowSql;
    Map<Integer, ImmutableSqlParameter> chunkParams;
    try {
      multiRowSql = InsertStatementParser.generateMultiRowInsert(insertInfo, chunkSize);
      chunkParams = new HashMap<>();
      int paramIndex = 1;
      int expectedParamCount = insertInfo.getColumnCount();

      for (int i = startIndex; i < endIndex; i++) {
        DatabricksParameterMetaData batchParams = batchParameterMetaData.get(i);
        Map<Integer, ImmutableSqlParameter> rowParams = batchParams.getParameterBindings();
        for (int j = 1; j <= rowParams.size(); j++) {
          if (rowParams.containsKey(j)) {
            chunkParams.put(paramIndex++, rowParams.get(j));
          }
        }
      }

      // Validate that we collected the expected number of parameters for the chunk
      int expectedTotalParams = chunkSize * expectedParamCount;
      if (chunkParams.size() != expectedTotalParams) {
        throw new DatabricksSQLException(
            "Parameter count mismatch: expected "
                + expectedTotalParams
                + " parameters for "
                + chunkSize
                + " rows, but got "
                + chunkParams.size(),
            DatabricksDriverErrorCode.BATCH_EXECUTE_EXCEPTION);
      }
    } catch (DatabricksSQLException e) {
      // If we can't generate SQL for this chunk, mark all rows as failed
      for (int i = startIndex; i < allUpdateCounts.length; i++) {
        allUpdateCounts[i] = Statement.EXECUTE_FAILED;
      }
      throw new DatabricksBatchUpdateException(
          "Failed to generate SQL for rows "
              + startIndex
              + "-"
              + (endIndex - 1)
              + " (inclusive): "
              + e.getMessage(),
          DatabricksDriverErrorCode.BATCH_EXECUTE_EXCEPTION.toString(),
          0,
          allUpdateCounts,
          e);
    }

    // Only retry at top level (depth 0) to avoid excessive retries on single rows
    int maxRetries = (depth == 0) ? 3 : 0;
    Exception lastException = null;

    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        if (attempt > 0) {
          LOGGER.warn(
              "Retrying chunk: rows {}-{} (inclusive) - attempt {}/{}",
              startIndex,
              endIndex - 1,
              attempt + 1,
              maxRetries + 1);
          // Simple exponential backoff: 100ms, 200ms, 400ms
          try {
            Thread.sleep(100L * (1L << (attempt - 1)));
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new DatabricksSQLException(
                "Retry sleep interrupted", ie, DatabricksDriverErrorCode.BATCH_EXECUTE_EXCEPTION);
          }
        }

        // Execute this chunk with interpolation if enabled
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

        LOGGER.debug(
            "Successfully processed chunk: rows {}-{} (inclusive)", startIndex, endIndex - 1);
        return chunkSize;

      } catch (Exception e) {
        lastException = e;
        LOGGER.error(
            "Failed to execute chunk: rows {}-{} (inclusive) - attempt {}/{}: {}",
            startIndex,
            endIndex - 1,
            attempt + 1,
            maxRetries + 1,
            e.getMessage());
      }
    }

    // Failed after all retries - try splitting the chunk if it has more than 1 row
    if (chunkSize > 1) {
      LOGGER.warn(
          "Chunk: rows {}-{} (inclusive) failed after {} attempts. Splitting into smaller chunks.",
          startIndex,
          endIndex - 1,
          maxRetries + 1);

      int midPoint = startIndex + (chunkSize / 2);

      // Recursively process first half
      int processedFirst =
          executeChunkWithRetryAndSplit(
              insertInfo,
              batchParameterMetaData,
              startIndex,
              midPoint,
              allUpdateCounts,
              interpolateParameters,
              depth + 1);

      // Recursively process second half
      int processedSecond =
          executeChunkWithRetryAndSplit(
              insertInfo,
              batchParameterMetaData,
              midPoint,
              endIndex,
              allUpdateCounts,
              interpolateParameters,
              depth + 1);

      return processedFirst + processedSecond;
    }

    // Single row that still failed - mark it and throw
    LOGGER.error(
        "Single row: rows {}-{} (inclusive) failed after {} attempts and cannot be split further",
        startIndex,
        endIndex - 1,
        maxRetries + 1);

    for (int i = startIndex; i < allUpdateCounts.length; i++) {
      allUpdateCounts[i] = Statement.EXECUTE_FAILED;
    }

    // Use null-safe error message
    String errorMsg =
        (lastException != null && lastException.getMessage() != null)
            ? lastException.getMessage()
            : String.valueOf(lastException);

    throw new DatabricksBatchUpdateException(
        "Rows "
            + startIndex
            + "-"
            + (endIndex - 1)
            + " (inclusive) failed after "
            + (maxRetries + 1)
            + " attempts: "
            + errorMsg,
        DatabricksDriverErrorCode.BATCH_EXECUTE_EXCEPTION.toString(),
        0,
        allUpdateCounts,
        lastException);
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
