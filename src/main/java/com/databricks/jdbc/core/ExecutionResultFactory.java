package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;

class ExecutionResultFactory {
  static IExecutionResult getResultSet(ResultData data, ResultManifest manifest, IDatabricksSession session) {
    // We will use Arrow Stream only in prod. JSON is for testing and prototype purpose
    switch (manifest.getFormat()) {
      case ARROW_STREAM:
        return new ArrowStreamResult(manifest, data, session);
      case JSON_ARRAY:
        // This is only for testing and prototype
        return new InlineJsonResult(manifest, data);
      default:
        throw new IllegalStateException("Invalid response format " + manifest.getFormat());
    }
  }
}
