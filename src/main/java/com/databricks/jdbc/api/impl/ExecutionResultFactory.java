package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.util.DatabricksThriftUtil.convertColumnarToRowBased;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.arrow.ArrowStreamResult;
import com.databricks.jdbc.api.impl.volume.VolumeOperationResult;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.util.DatabricksThriftUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotImplementedException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.model.client.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.model.client.thrift.generated.TRowSet;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import java.sql.SQLException;
import java.util.List;

class ExecutionResultFactory {
  static IExecutionResult getResultSet(
      ResultData data,
      ResultManifest manifest,
      StatementId statementId,
      IDatabricksSession session,
      IDatabricksStatementInternal statement)
      throws DatabricksParsingException {
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
      throws DatabricksParsingException {
    if (manifest.getFormat() == null) {
      throw new IllegalStateException("Empty response format");
    }
    // We use JSON_ARRAY for metadata and update commands, and ARROW_STREAM for query results
    switch (manifest.getFormat()) {
      case ARROW_STREAM:
        return new ArrowStreamResult(manifest, data, statementId, session);
      case JSON_ARRAY:
        // This is used for metadata and update commands
        return new InlineJsonResult(manifest, data);
      default:
        throw new IllegalStateException("Invalid response format " + manifest.getFormat());
    }
  }

  static IExecutionResult getResultSet(
      TRowSet data,
      TGetResultSetMetadataResp manifest,
      StatementId statementId,
      IDatabricksSession session,
      IDatabricksStatementInternal statement)
      throws SQLException {
    IExecutionResult resultHandler = getResultHandler(data, manifest, statementId, session);
    if (manifest.isSetIsStagingOperation() && manifest.isIsStagingOperation()) {
      return new VolumeOperationResult(
          DatabricksThriftUtil.getRowCount(data),
          manifest.getSchema().getColumnsSize(),
          session,
          resultHandler,
          statement);
    } else {
      return resultHandler;
    }
  }

  private static IExecutionResult getResultHandler(
      TRowSet data,
      TGetResultSetMetadataResp manifest,
      StatementId statementId,
      IDatabricksSession session)
      throws SQLException {
    switch (manifest.getResultFormat()) {
      case COLUMN_BASED_SET:
        return getResultSet(convertColumnarToRowBased(data));
      case ARROW_BASED_SET:
        return new ArrowStreamResult(manifest, data, true, statementId, session);
      case URL_BASED_SET:
        return new ArrowStreamResult(manifest, data, false, statementId, session);
      case ROW_BASED_SET:
        throw new DatabricksSQLFeatureNotSupportedException(
            "Invalid state - row based set cannot be received");
      default:
        throw new DatabricksSQLFeatureNotImplementedException(
            "Invalid thrift response format " + manifest.getResultFormat());
    }
  }

  static IExecutionResult getResultSet(Object[][] rows) {
    return new InlineJsonResult(rows);
  }

  static IExecutionResult getResultSet(List<List<Object>> rows) {
    return new InlineJsonResult(rows);
  }
}
