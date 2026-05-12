package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.util.DecompressionUtil.decompress;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;

/** Class to manage inline Arrow chunks */
public class InlineChunkProvider implements ChunkProvider {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(InlineChunkProvider.class);
  private long totalRows;
  private long currentChunkIndex;
  private boolean isClosed;
  private final ArrowResultChunk
      arrowResultChunk; // There is only one packet of data in case of inline arrow

  /**
   * Constructor for inline arrow chunk provider from {@link ResultData} and {@link ResultManifest}.
   *
   * @param resultData Data object containing the result data
   * @param resultManifest Manifest object containing the result metadata
   * @throws DatabricksSQLException if there is an error in processing the inline arrow data
   */
  InlineChunkProvider(ResultData resultData, ResultManifest resultManifest)
      throws DatabricksSQLException {
    this.currentChunkIndex = -1;
    this.totalRows = resultManifest.getTotalRowCount();

    // Decompress the inline data if applicable and create an ArrowResultChunk
    CompressionCodec compressionType = resultManifest.getResultCompression();
    byte[] decompressedBytes =
        decompress(
            resultData.getAttachment(),
            compressionType,
            "Data fetch for inline arrow batch with decompression algorithm : " + compressionType);
    this.arrowResultChunk =
        ArrowResultChunk.builder()
            .withInputStream(new ByteArrayInputStream(decompressedBytes), totalRows)
            .build();
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNextChunk() {
    return this.currentChunkIndex == -1;
  }

  /** {@inheritDoc} */
  @Override
  public boolean next() {
    if (!hasNextChunk()) {
      return false;
    }
    this.currentChunkIndex++;
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public ArrowResultChunk getChunk() {
    return arrowResultChunk;
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    isClosed = true;
    arrowResultChunk.releaseChunk();
  }

  @Override
  public long getRowCount() {
    return totalRows;
  }

  @Override
  public long getChunkCount() {
    return 0;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @VisibleForTesting
  void handleError(Exception e) throws DatabricksParsingException {
    String errorMessage =
        String.format("Cannot process inline arrow format. Error: %s", e.getMessage());
    LOGGER.error(errorMessage);
    throw new DatabricksParsingException(
        errorMessage, e, DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
  }
}
