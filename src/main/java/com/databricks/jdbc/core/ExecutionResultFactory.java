package com.databricks.jdbc.core;

import com.databricks.jdbc.client.impl.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.client.impl.thrift.generated.TRowSet;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.ResultManifest;
import java.util.List;

class ExecutionResultFactory {
  static IExecutionResult getResultSet(
      ResultData data, ResultManifest manifest, String statementId, IDatabricksSession session) {
    // Return Volume operation handler
    if (manifest.getIsVolumeOperation() != null && manifest.getIsVolumeOperation()) {
      return new VolumeOperationResult(data, statementId, session);
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

  static IExecutionResult getResultSet(TRowSet data, TGetResultSetMetadataResp manifest) {
    return new InlineJsonResult(manifest, data);
  }

  static IExecutionResult getResultSet(Object[][] rows) {
    return new InlineJsonResult(rows);
  }

  static IExecutionResult getResultSet(List<List<Object>> rows) {
    return new InlineJsonResult(rows);
  }
}
