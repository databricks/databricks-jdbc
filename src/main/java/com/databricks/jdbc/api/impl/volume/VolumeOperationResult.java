package com.databricks.jdbc.api.impl.volume;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.ALLOWED_STAGING_INGESTION_PATHS;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.ALLOWED_VOLUME_INGESTION_PATHS;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.IExecutionResult;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.ErrorCodes;
import com.databricks.jdbc.common.util.VolumeUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ResultManifest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.entity.InputStreamEntity;

/** Class to handle the result of a volume operation */
public class VolumeOperationResult implements IExecutionResult {

  private final IDatabricksSession session;
  private final IExecutionResult resultHandler;
  private final IDatabricksStatementInternal statement;
  private final IDatabricksHttpClient httpClient;
  private final long rowCount;
  private final long columnCount;

  private VolumeOperationProcessor volumeOperationProcessor;
  private int currentRowIndex;
  private VolumeInputStream volumeInputStream = null;
  private long volumeStreamContentLength = -1L;

  public VolumeOperationResult(
      long totalRows,
      long totalColumns,
      IDatabricksSession session,
      IExecutionResult resultHandler,
      IDatabricksStatementInternal statement) {
    this.rowCount = totalRows;
    this.columnCount = totalColumns;
    this.session = session;
    this.resultHandler = resultHandler;
    this.statement = statement;
    this.httpClient =
        DatabricksHttpClientFactory.getInstance().getClient(session.getConnectionContext());
    this.currentRowIndex = -1;
  }

  @VisibleForTesting
  VolumeOperationResult(
      ResultManifest manifest,
      IDatabricksSession session,
      IExecutionResult resultHandler,
      IDatabricksHttpClient httpClient,
      IDatabricksStatementInternal statement) {
    this.rowCount = manifest.getTotalRowCount();
    this.columnCount = manifest.getSchema().getColumnCount();
    this.session = session;
    this.resultHandler = resultHandler;
    this.statement = statement;
    this.httpClient = httpClient;
    this.currentRowIndex = -1;
  }

  private void initHandler(IExecutionResult resultHandler) throws DatabricksSQLException {
    VolumeUtil.VolumeOperationType operation =
        VolumeUtil.VolumeOperationType.fromString(getString(resultHandler.getObject(0)));
    String presignedUrl = getString(resultHandler.getObject(1));
    String localFile = columnCount > 3 ? getString(resultHandler.getObject(3)) : null;
    Map<String, String> headers = getHeaders(getString(resultHandler.getObject(2)));
    String allowedVolumeIngestionPaths = getAllowedVolumeIngestionPaths();
    this.volumeOperationProcessor =
        VolumeOperationProcessor.Builder.createBuilder()
            .operationType(operation)
            .operationUrl(presignedUrl)
            .headers(headers)
            .localFilePath(localFile)
            .allowedVolumeIngestionPathString(allowedVolumeIngestionPaths)
            .isAllowedInputStreamForVolumeOperation(
                statement.isAllowedInputStreamForVolumeOperation())
            .inputStream(statement.getInputStreamForUCVolume())
            .databricksHttpClient(httpClient)
            .getStreamReceiver(
                (entity) -> {
                  try {
                    this.setVolumeOperationEntityStream(entity);
                  } catch (Exception e) {
                    throw new RuntimeException(
                        "Failed to set result set volumeOperationEntityStream", e);
                  }
                })
            .build();
  }

  private String getAllowedVolumeIngestionPaths() {
    String allowedPaths =
        session.getClientInfoProperties().get(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase());
    if (Strings.isNullOrEmpty(allowedPaths)) {
      allowedPaths =
          session.getClientInfoProperties().getOrDefault(ALLOWED_STAGING_INGESTION_PATHS, "");
    }
    return allowedPaths;
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
          throw new DatabricksSQLException(
              "Failed to parse headers", e, ErrorCodes.VOLUME_OPERATION_PARSING_ERROR);
        }
      }
    }
    return new HashMap<>();
  }

  private void validateMetadata() throws DatabricksSQLException {
    // For now, we only support one row for Volume operation
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
      return volumeOperationProcessor.getStatus().name();
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
      volumeOperationProcessor.process();

      if (volumeOperationProcessor.getStatus() == VolumeUtil.VolumeOperationStatus.FAILED) {
        throw new DatabricksSQLException(
            "Volume operation failed: " + volumeOperationProcessor.getErrorMessage());
      }
      if (volumeOperationProcessor.getStatus() == VolumeUtil.VolumeOperationStatus.ABORTED) {
        throw new DatabricksSQLException(
            "Volume operation aborted: " + volumeOperationProcessor.getErrorMessage());
      }
      currentRowIndex++;
      return true;
    } else {
      return false;
    }
  }

  public void setVolumeOperationEntityStream(HttpEntity httpEntity) throws IOException {
    this.volumeInputStream = new VolumeInputStream(httpEntity);
    this.volumeStreamContentLength = httpEntity.getContentLength();
  }

  public InputStreamEntity getVolumeOperationInputStream() {
    return new InputStreamEntity(this.volumeInputStream, this.volumeStreamContentLength);
  }

  @Override
  public boolean hasNext() {
    return resultHandler.hasNext();
  }

  @Override
  public void close() {
    resultHandler.close();
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public long getChunkCount() {
    return 0;
  }
}
