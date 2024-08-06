package com.databricks.jdbc.core;

import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.convertColumnarToRowBased;

import com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper;
import com.databricks.jdbc.client.impl.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.client.impl.thrift.generated.TRowSet;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.ResultManifest;
import java.util.List;

class ExecutionResultFactory {
  static IExecutionResult getResultSet(
      ResultData data,
      ResultManifest manifest,
      String statementId,
      IDatabricksSession session,
      IDatabricksStatement statement,
      IDatabricksResultSet resultSet) {
    IExecutionResult resultHandler = getResultHandler(data, manifest, statementId, session);
    if (manifest.getIsVolumeOperation() != null && manifest.getIsVolumeOperation()) {
      return new VolumeOperationResult(
          statementId,
          manifest.getTotalRowCount(),
          manifest.getSchema().getColumnCount(),
          session,
          resultHandler,
          statement,
          resultSet);
    } else {
      return resultHandler;
    }
  }

  private static IExecutionResult getResultHandler(
      ResultData data, ResultManifest manifest, String statementId, IDatabricksSession session) {
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
      String statementId,
      IDatabricksSession session,
      IDatabricksStatement statement,
      IDatabricksResultSet resultSet)
      throws DatabricksSQLException {
    IExecutionResult resultHandler = getResultHandler(data, manifest, statementId, session);
    if (manifest.isSetIsStagingOperation() && manifest.isIsStagingOperation()) {
      return new VolumeOperationResult(
          statementId,
          DatabricksThriftHelper.getRowCount(data),
          manifest.getSchema().getColumnsSize(),
          session,
          resultHandler,
          statement,
          resultSet);
    } else {
      return resultHandler;
    }
  }

  private static IExecutionResult getResultHandler(
      TRowSet data,
      TGetResultSetMetadataResp manifest,
      String statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
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
