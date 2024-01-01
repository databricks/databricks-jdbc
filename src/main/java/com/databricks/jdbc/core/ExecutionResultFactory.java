package com.databricks.jdbc.core;

import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import java.util.List;

class ExecutionResultFactory {
  static IExecutionResult getResultSet(
      ResultData data, ResultManifest manifest, String statementId, IDatabricksSession session) {
    // We will use Arrow Stream only in prod. JSON is for testing and prototype purpose
    switch (manifest.getFormat()) {
      case ARROW_STREAM:
        return new ArrowStreamResult(manifest, data, statementId, session);
      case JSON_ARRAY:
        // This is only for testing and prototype
        return new InlineJsonResult(manifest, data);
      default:
        throw new IllegalStateException("Invalid response format " + manifest.getFormat());
    }
  }

  static IExecutionResult getResultSet(Object[][] rows) {
    return new InlineJsonResult(rows);
  }

  static IExecutionResult getResultSet(List<List<Object>> rows) {
    return new InlineJsonResult(rows);
  }
}
