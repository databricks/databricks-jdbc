package com.databricks.jdbc.exception;

import static com.databricks.jdbc.telemetry.TelemetryHelper.exportFailureLog;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;

/** Exception thrown when chunk download fails */
public class DatabricksChunkDownloadException extends SQLException {

  public DatabricksChunkDownloadException(
      String reason, int chunkId, int retryCount, IDatabricksConnectionContext connectionContext) {
    super(reason, DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR.toString());
    exportFailureLog(
        connectionContext, DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR.toString(), reason);
  }
}
