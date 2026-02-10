package com.databricks.jdbc.api.impl.volume;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;

import com.databricks.jdbc.api.impl.IExecutionResult;
import com.databricks.jdbc.api.impl.VolumeOperationStatus;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.common.util.VolumeUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksVolumeOperationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.entity.InputStreamEntity;

/** Class to handle the result of a volume operation */
public class VolumeOperationResult implements IExecutionResult {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(VolumeOperationResult.class);
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
      IDatabricksStatementInternal statement)
      throws SQLException {
    this.rowCount = totalRows;
    this.columnCount = totalColumns;
    this.session = session;
    this.resultHandler = resultHandler;
    this.statement = statement;
    this.httpClient =
        DatabricksHttpClientFactory.getInstance()
            .getClient(session.getConnectionContext(), HttpClientType.VOLUME);
    this.currentRowIndex = -1;
    completeVolumeOperation();
  }

  @VisibleForTesting
  VolumeOperationResult(
      ResultManifest manifest,
      IDatabricksSession session,
      IExecutionResult resultHandler,
      IDatabricksHttpClient httpClient,
      IDatabricksStatementInternal statement)
      throws SQLException {
    this.rowCount = manifest.getTotalRowCount();
    this.columnCount = manifest.getSchema().getColumnCount();
    this.session = session;
    this.resultHandler = resultHandler;
    this.statement = statement;
    this.httpClient = httpClient;
    this.currentRowIndex = -1;
    completeVolumeOperation();
  }

  private void initHandler(IExecutionResult resultHandler) throws DatabricksSQLException {
    VolumeUtil.VolumeOperationType operation =
        VolumeUtil.VolumeOperationType.fromString(getString(resultHandler.getObject(0)));
    String presignedUrl = getString(resultHandler.getObject(1));
    String localFile = columnCount > 3 ? getString(resultHandler.getObject(3)) : null;
    Map<String, String> headers = getHeaders(getString(resultHandler.getObject(2)));
    String allowedVolumeIngestionPaths = getAllowedVolumeIngestionPaths();
    boolean volumeOperationsAllowed = isEnableVolumeOperations();
    this.volumeOperationProcessor =
        VolumeOperationProcessor.Builder.createBuilder()
            .operationType(operation)
            .operationUrl(presignedUrl)
            .headers(headers)
            .localFilePath(localFile)
            .allowedVolumeIngestionPathString(allowedVolumeIngestionPaths)
            .isAllowedInputStreamForVolumeOperation(
                statement.isAllowedInputStreamForVolumeOperation())
            .isEnableVolumeOperations(volumeOperationsAllowed)
            .inputStream(statement.getInputStreamForUCVolume())
            .databricksHttpClient(httpClient)
            .getStreamReceiver(
                (entity) -> {
                  try {
                    this.setVolumeOperationEntityStream(entity);
                  } catch (Exception e) {
                    String message =
                        String.format(
                            "Failed to set result set volumeOperationEntityStream %s",
                            e.getMessage());
                    LOGGER.error(e, message);
                    throw new DatabricksDriverException(
                        message, DatabricksDriverErrorCode.VOLUME_OPERATION_EXCEPTION);
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
    if (Strings.isNullOrEmpty(allowedPaths)) {
      allowedPaths = session.getConnectionContext().getVolumeOperationAllowedPaths();
    }
    return allowedPaths;
  }

  private boolean isEnableVolumeOperations() {
    String enableVolumeOperations =
        session.getClientInfoProperties().get(ENABLE_VOLUME_OPERATIONS.toLowerCase());
    if (enableVolumeOperations == null) {
      return false;
    }
    String value = enableVolumeOperations.trim();
    return value.equalsIgnoreCase("true") || value.equals("1");
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
        try {
          return JsonUtil.getMapper().readValue(headers, Map.class);
        } catch (JsonProcessingException e) {
          throw new DatabricksVolumeOperationException(
              "Failed to parse headers",
              e,
              DatabricksDriverErrorCode.VOLUME_OPERATION_PARSING_ERROR);
        }
      }
    }
    return new HashMap<>();
  }

  private void validateMetadata() throws DatabricksSQLException {
    // For now, we only support one row for Volume operation
    String errorMessage = null;
    if (rowCount > 1) {
      errorMessage = "Too many rows for Volume Operation";
    } else if (columnCount > 4) {
      errorMessage = "Too many columns for Volume Operation";
    } else if (columnCount < 3) {
      errorMessage = "Too few columns for Volume Operation";
    }
    if (errorMessage != null) {
      throw new DatabricksVolumeOperationException(
          errorMessage, DatabricksDriverErrorCode.VOLUME_OPERATION_INVALID_STATE);
    }
  }

  @Override
  public Object getObject(int columnIndex) throws DatabricksSQLException {
    if (columnIndex == 0) {
      return volumeOperationProcessor.getStatus().name();
    }
    String userMessage = (currentRowIndex < 0) ? "Invalid row access" : "Invalid column access";
    String errorMessage =
        String.format(
            "%s, Row: %s, Column: %s, statementID %s",
            userMessage, currentRowIndex, columnIndex, statement.getStatementId());
    LOGGER.error(errorMessage);
    throw new DatabricksVolumeOperationException(
        errorMessage, DatabricksDriverErrorCode.VOLUME_OPERATION_INVALID_STATE);
  }

  @Override
  public long getCurrentRow() {
    return currentRowIndex;
  }

  @Override
  public boolean next() throws DatabricksSQLException {
    if (currentRowIndex == -1) {
      currentRowIndex++;
      return true;
    } else {
      return false;
    }
  }

  private void completeVolumeOperation() throws SQLException {
    while (resultHandler.hasNext()) {
      validateMetadata();
      resultHandler.next();
      initHandler(resultHandler);
      volumeOperationProcessor.process();
      ensureSuccessVolumeProcessorStatus();
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
    return currentRowIndex == -1;
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

  private void ensureSuccessVolumeProcessorStatus() throws DatabricksVolumeOperationException {
    if (volumeOperationProcessor.getStatus() == VolumeOperationStatus.FAILED
        || volumeOperationProcessor.getStatus() == VolumeOperationStatus.ABORTED) {
      String errorMessage =
          String.format(
              "Volume operation status : %s, Error message: %s",
              volumeOperationProcessor.getStatus(), volumeOperationProcessor.getErrorMessage());
      LOGGER.error(errorMessage);
      throw new DatabricksVolumeOperationException(
          errorMessage, DatabricksDriverErrorCode.VOLUME_OPERATION_EXCEPTION);
    }
  }
}
