package com.databricks.jdbc.core;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.ALLOWED_VOLUME_INGESTION_PATHS;

import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.VolumeOperationInfo;
import com.databricks.jdbc.core.VolumeOperationExecutor.VolumeOperationStatus;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;

/** Class to handle the result of a volume operation */
class VolumeOperationResult implements IExecutionResult {

  private final IDatabricksSession session;
  private final String statementId;
  private VolumeOperationExecutor volumeOperationExecutor;
  private int currentRowIndex;

  VolumeOperationResult(ResultData resultData, String statementId, IDatabricksSession session) {
    this.statementId = statementId;
    this.session = session;
    this.currentRowIndex = -1;
    init(
        resultData.getVolumeOperationInfo(),
        DatabricksHttpClient.getInstance(session.getConnectionContext()));
  }

  @VisibleForTesting
  VolumeOperationResult(
      ResultData resultData,
      String statementId,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient) {
    this.statementId = statementId;
    this.session = session;
    this.currentRowIndex = -1;
    init(resultData.getVolumeOperationInfo(), httpClient);
  }

  private void init(VolumeOperationInfo volumeOperationInfo, IDatabricksHttpClient httpClient) {
    this.volumeOperationExecutor =
        new VolumeOperationExecutor(
            volumeOperationInfo.getVolumeOperationType(),
            volumeOperationInfo.getPresignedUrl().getExternalLink(),
            volumeOperationInfo.getLocalFile(),
            volumeOperationInfo.getPresignedUrl().getHttpHeaders(),
            session
                .getClientInfoProperties()
                .getOrDefault(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ""),
            httpClient);
    Thread thread = new Thread(volumeOperationExecutor);
    thread.setName("VolumeOperationExecutor " + statementId);
    thread.start();
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    if (currentRowIndex < 0) {
      throw new DatabricksSQLException("Invalid row access");
    }
    if (columnIndex == 1) {
      return volumeOperationExecutor.getStatus().name();
    } else {
      throw new DatabricksSQLException("Invalid column access");
    }
  }

  @Override
  public long getCurrentRow() {
    return currentRowIndex;
  }

  @Override
  public boolean next() throws DatabricksSQLException {
    if (hasNext()) {
      poll();
      currentRowIndex++;
      return true;
    } else {
      return false;
    }
  }

  private void poll() throws DatabricksSQLException {
    // TODO: handle timeouts
    while (volumeOperationExecutor.getStatus() == VolumeOperationStatus.PENDING
        || volumeOperationExecutor.getStatus() == VolumeOperationStatus.RUNNING) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new DatabricksSQLException(
            "Thread interrupted while waiting for volume operation to complete", e);
      }
    }
    if (volumeOperationExecutor.getStatus() == VolumeOperationStatus.FAILED) {
      throw new DatabricksSQLException(
          "Volume operation failed: " + volumeOperationExecutor.getErrorMessage());
    }
    if (volumeOperationExecutor.getStatus() == VolumeOperationStatus.ABORTED) {
      throw new DatabricksSQLException(
          "Volume operation aborted: " + volumeOperationExecutor.getErrorMessage());
    }
  }

  @Override
  public boolean hasNext() {
    return currentRowIndex < 0;
  }

  @Override
  public void close() {
    // TODO: handle close, shall we abort the operation?
  }
}
