package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.util.DecompressionUtil.decompress;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import java.io.ByteArrayInputStream;

/**
 * ChunkProvider for SEA inline Arrow results (bounded SEA API). Fetches chunks lazily via
 * GetResultData with row_offset, similar to Thrift's LazyThriftInlineArrowResult but using SEA's
 * ArrowIPC format in the attachment field.
 *
 * <p>Used when UseBoundedSeaApi=1 AND EnableQueryResultDownload=0 (cloud fetch disabled).
 */
class SeaInlineArrowChunkProvider implements ChunkProvider {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(SeaInlineArrowChunkProvider.class);

  private final IDatabricksSession session;
  private final StatementId statementId;
  private final CompressionCodec compressionCodec;

  private ArrowResultChunk currentChunk;
  private long currentChunkIndex;
  private long nextChunkIndex;
  private long nextRowOffset;
  private boolean hasMore;
  private long totalRowCount;
  private boolean isClosed;

  /**
   * @param initialResultData The first chunk from the ExecuteStatement response
   * @param resultManifest Manifest with compression and schema info
   * @param statementId Statement ID for subsequent fetch calls
   * @param session Session for making GetResultData calls
   */
  SeaInlineArrowChunkProvider(
      ResultData initialResultData,
      ResultManifest resultManifest,
      StatementId statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
    this.session = session;
    this.statementId = statementId;
    this.compressionCodec = resultManifest.getResultCompression();
    this.currentChunkIndex = -1;
    this.isClosed = false;
    this.totalRowCount = 0;

    // Process initial chunk from the execute response
    this.currentChunk = processResultData(initialResultData);
    this.hasMore = initialResultData.getNextChunkIndex() != null;
    if (hasMore) {
      this.nextChunkIndex = initialResultData.getNextChunkIndex();
    }
    long rowCount = initialResultData.getRowCount() != null ? initialResultData.getRowCount() : 0;
    long rowOffset =
        initialResultData.getRowOffset() != null ? initialResultData.getRowOffset() : 0;
    this.nextRowOffset = rowOffset + rowCount;
    this.totalRowCount += rowCount;

    LOGGER.debug(
        "SeaInlineArrowChunkProvider created for statement {}: hasMore={}, nextChunkIndex={}, nextRowOffset={}",
        statementId.toSQLExecStatementId(),
        hasMore,
        nextChunkIndex,
        nextRowOffset);
  }

  @Override
  public boolean hasNextChunk() {
    // First call: initial chunk not yet consumed
    if (currentChunkIndex == -1) {
      return true;
    }
    return hasMore;
  }

  @Override
  public boolean next() throws DatabricksSQLException {
    if (currentChunkIndex == -1) {
      // First call — return the initial chunk (already loaded)
      currentChunkIndex = 0;
      return true;
    }

    if (!hasMore) {
      return false;
    }

    // Fetch next chunk via GetResultData
    try {
      DatabricksSdkClient client = (DatabricksSdkClient) session.getDatabricksClient();
      ResultData resultData =
          client.getResultChunksData(statementId, nextChunkIndex, nextRowOffset);

      // Release previous chunk
      if (currentChunk != null) {
        currentChunk.releaseChunk();
      }

      currentChunk = processResultData(resultData);
      currentChunkIndex = nextChunkIndex;

      // Update continuation from response
      hasMore = resultData.getNextChunkIndex() != null;
      if (hasMore) {
        nextChunkIndex = resultData.getNextChunkIndex();
      }
      long rowCount = resultData.getRowCount() != null ? resultData.getRowCount() : 0;
      nextRowOffset += rowCount;
      totalRowCount += rowCount;

      LOGGER.debug(
          "Fetched inline chunk {}: rowCount={}, hasMore={}, nextRowOffset={}",
          currentChunkIndex,
          rowCount,
          hasMore,
          nextRowOffset);

      return true;
    } catch (Exception e) {
      throw new DatabricksSQLException(
          "Failed to fetch inline Arrow chunk " + nextChunkIndex + ": " + e.getMessage(),
          e,
          com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
  }

  @Override
  public ArrowResultChunk getChunk() {
    return currentChunk;
  }

  @Override
  public void close() {
    isClosed = true;
    if (currentChunk != null) {
      currentChunk.releaseChunk();
      currentChunk = null;
    }
  }

  @Override
  public long getRowCount() {
    return totalRowCount;
  }

  @Override
  public long getChunkCount() {
    return currentChunkIndex + 1;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  /** Decompresses attachment bytes and creates an ArrowResultChunk. */
  private ArrowResultChunk processResultData(ResultData resultData) throws DatabricksSQLException {
    byte[] attachment = resultData.getAttachment();
    if (attachment == null || attachment.length == 0) {
      throw new DatabricksSQLException(
          "No inline Arrow data (attachment) in result for chunk",
          com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode.RESULT_SET_ERROR);
    }

    byte[] decompressedBytes =
        decompress(attachment, compressionCodec, "SEA inline Arrow chunk decompression");

    long rowCount = resultData.getRowCount() != null ? resultData.getRowCount() : 0;
    return ArrowResultChunk.builder()
        .withInputStream(new ByteArrayInputStream(decompressedBytes), rowCount)
        .build();
  }
}
