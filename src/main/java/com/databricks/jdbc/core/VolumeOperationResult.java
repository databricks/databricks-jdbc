package com.databricks.jdbc.core;

import static com.databricks.jdbc.commons.EnvironmentVariables.DEFAULT_SLEEP_DELAY;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.ALLOWED_VOLUME_INGESTION_PATHS;

import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.client.sqlexec.ResultManifest;
import com.databricks.jdbc.core.VolumeOperationExecutor.VolumeOperationStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;

/** Class to handle the result of a volume operation */
class VolumeOperationResult implements IExecutionResult {

  private final IDatabricksSession session;
  private final String statementId;
  private final IExecutionResult resultHandler;
  private final IDatabricksHttpClient httpClient;
  private final long rowCount;
  private final long columnCount;

  private VolumeOperationExecutor volumeOperationExecutor;
  private int currentRowIndex;

  VolumeOperationResult(
      String statementId,
      long totalRows,
      long totalColumns,
      IDatabricksSession session,
      IExecutionResult resultHandler) {
    this.statementId = statementId;
    this.rowCount = totalRows;
    this.columnCount = totalColumns;
    this.session = session;
    this.resultHandler = resultHandler;
    this.httpClient = DatabricksHttpClient.getInstance(session.getConnectionContext());
    this.currentRowIndex = -1;
  }

  @VisibleForTesting
  VolumeOperationResult(
      String statementId,
      ResultManifest manifest,
      IDatabricksSession session,
      IExecutionResult resultHandler,
      IDatabricksHttpClient httpClient) {
    this.statementId = statementId;
    this.rowCount = manifest.getTotalRowCount();
    this.columnCount = manifest.getSchema().getColumnCount();
    this.session = session;
    this.resultHandler = resultHandler;
    this.httpClient = httpClient;
    this.currentRowIndex = -1;
  }

  private void initHandler(IExecutionResult resultHandler) throws DatabricksSQLException {
    String operation = getString(resultHandler.getObject(0));
    String presignedUrl = getString(resultHandler.getObject(1));
    String localFile = columnCount > 3 ? getString(resultHandler.getObject(3)) : null;
    Map<String, String> headers = getHeaders(getString(resultHandler.getObject(2)));
    this.volumeOperationExecutor =
        new VolumeOperationExecutor(
            operation,
            presignedUrl,
            headers,
            localFile,
            session
                .getClientInfoProperties()
                .getOrDefault(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ""),
            httpClient);
    Thread thread = new Thread(volumeOperationExecutor);
    thread.setName("VolumeOperationExecutor " + statementId);
    thread.start();
  }

  private String getString(Object obj) {
    return obj == null ? null : obj.toString();
  }

  private Map<String, String> getHeaders(String headersVal) throws DatabricksSQLException {
    if (headersVal != null && !headersVal.isEmpty()) {
      // Map is encoded in extra [] while doing toString
      String headers =
          headersVal.charAt(0) == '['
              ? headersVal.substring(1, headersVal.length() - 1)
              : headersVal;
      if (!headers.isEmpty()) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
          return objectMapper.readValue(headers, Map.class);
        } catch (JsonProcessingException e) {
          throw new DatabricksSQLException("Failed to parse headers", e);
        }
      }
    }
    return new HashMap<>();
  }

  private void validateMetadata() throws DatabricksSQLException {
    // For now we only support one row for Volume operation
    if (rowCount > 1) {
      throw new DatabricksSQLException("Too many rows for Volume Operation");
    }
    if (columnCount > 4) {
      throw new DatabricksSQLException("Too many columns for Volume Operation");
    }
    if (columnCount < 3) {
      throw new DatabricksSQLException("Too few columns for Volume Operation");
    }
  }

  @Override
  public Object getObject(int columnIndex) throws DatabricksSQLException {
    if (currentRowIndex < 0) {
      throw new DatabricksSQLException("Invalid row access");
    }
    if (columnIndex == 0) {
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
      validateMetadata();
      resultHandler.next();
      initHandler(resultHandler);

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
        Thread.sleep(DEFAULT_SLEEP_DELAY);
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
    return resultHandler.hasNext();
  }

  @Override
  public void close() {
    // TODO: handle close, shall we abort the operation?
  }
}
