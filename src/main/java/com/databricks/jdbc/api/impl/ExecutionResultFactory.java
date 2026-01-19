package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.impl.arrow.ArrowStreamResult;
import com.databricks.jdbc.api.impl.arrow.LazyThriftInlineArrowResult;
import com.databricks.jdbc.api.impl.volume.VolumeOperationResult;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.util.DatabricksThriftUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.*;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TSparkRowSetType;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.latency.TelemetryCollector;
import java.sql.SQLException;
import java.util.List;

class ExecutionResultFactory {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(ExecutionResultFactory.class);

  static IExecutionResult getResultSet(
      ResultData data,
      ResultManifest manifest,
      StatementId statementId,
      IDatabricksSession session,
      IDatabricksStatementInternal statement)
      throws DatabricksSQLException {
    IExecutionResult resultHandler = getResultHandler(data, manifest, statementId, session);
    if (manifest.getIsVolumeOperation() != null && manifest.getIsVolumeOperation()) {
      return new VolumeOperationResult(
          manifest.getTotalRowCount(),
          manifest.getSchema().getColumnCount(),
          session,
          resultHandler,
          statement);
    } else {
      return resultHandler;
    }
  }

  private static IExecutionResult getResultHandler(
      ResultData data, ResultManifest manifest, StatementId statementId, IDatabricksSession session)
      throws DatabricksSQLException {
    if (manifest.getFormat() == null) {
      throw new DatabricksParsingException(
          "Empty response format", DatabricksDriverErrorCode.INVALID_STATE);
    }
    TelemetryCollector.getInstance().setResultFormat(statementId, manifest.getFormat());
    LOGGER.info("Processing result of format {} from SQL Execution API", manifest.getFormat());
    // We use JSON_ARRAY for metadata and update commands, and ARROW_STREAM for query results
    switch (manifest.getFormat()) {
      case ARROW_STREAM:
        return new ArrowStreamResult(manifest, data, statementId, session);
      case JSON_ARRAY:
        // This is used for metadata and update commands
        return new InlineJsonResult(manifest, data, statementId, session);
      default:
        String errorMessage = String.format("Invalid response format %s", manifest.getFormat());
        LOGGER.error(errorMessage);
        throw new DatabricksParsingException(errorMessage, DatabricksDriverErrorCode.INVALID_STATE);
    }
  }

  static IExecutionResult getResultSet(
      TFetchResultsResp resultsResp,
      IDatabricksSession session,
      IDatabricksStatementInternal statement)
      throws SQLException {
    IExecutionResult resultHandler = getResultHandler(resultsResp, statement, session);
    if (resultsResp.getResultSetMetadata().isSetIsStagingOperation()
        && resultsResp.getResultSetMetadata().isIsStagingOperation()) {
      return new VolumeOperationResult(
          DatabricksThriftUtil.getRowCount(resultsResp.getResults()),
          resultsResp.getResultSetMetadata().getSchema().getColumnsSize(),
          session,
          resultHandler,
          statement);
    } else {
      return resultHandler;
    }
  }

  private static IExecutionResult getResultHandler(
      TFetchResultsResp resultsResp,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session)
      throws SQLException {
    TSparkRowSetType resultFormat = resultsResp.getResultSetMetadata().getResultFormat();
    TelemetryCollector.getInstance().setResultFormat(parentStatement, resultFormat);
    LOGGER.info("Processing result of format {} from Thrift server", resultFormat);
    switch (resultFormat) {
      case COLUMN_BASED_SET:
        return new LazyThriftResult(resultsResp, parentStatement, session);
      case ARROW_BASED_SET:
        return new LazyThriftInlineArrowResult(resultsResp, parentStatement, session);
      case URL_BASED_SET:
        return new ArrowStreamResult(resultsResp, parentStatement, session);
      case ROW_BASED_SET:
        throw new DatabricksSQLFeatureNotSupportedException(
            "Invalid state - row based set cannot be received");
      default:
        throw new DatabricksSQLFeatureNotImplementedException(
            "Invalid thrift response format " + resultFormat);
    }
  }

  static IExecutionResult getResultSet(Object[][] rows) {
    return new InlineJsonResult(rows);
  }

  static IExecutionResult getResultSet(List<List<Object>> rows) {
    return new InlineJsonResult(rows);
  }
}
