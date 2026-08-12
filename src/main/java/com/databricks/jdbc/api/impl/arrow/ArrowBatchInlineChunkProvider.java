package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.util.ArrowUtil.createArrowBatchByteStream;

import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ArrowBatch;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.List;

/** Provides one inline chunk assembled from APC native Arrow record-batch messages. */
class ArrowBatchInlineChunkProvider implements ChunkProvider {
  private final long totalRows;
  private final ArrowResultChunk arrowResultChunk;
  private long currentChunkIndex = -1;
  private boolean isClosed;

  ArrowBatchInlineChunkProvider(ResultData resultData, ResultManifest resultManifest)
      throws DatabricksSQLException {
    if (resultData == null) {
      throw parsingError("ARROW_BATCH response is missing result data");
    }
    List<ArrowBatch> arrowBatches = resultData.getArrowBatches();
    this.totalRows = validateAndCountRows(arrowBatches, resultData, resultManifest);
    try {
      this.arrowResultChunk =
          ArrowResultChunk.builder()
              .withInputStream(
                  createArrowBatchByteStream(resultManifest.getSchema(), arrowBatches), totalRows)
              .build();
    } catch (DatabricksParsingException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DatabricksParsingException(
          "Failed to parse native Arrow batches: " + e.getMessage(),
          e,
          DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
    }
  }

  private static long validateAndCountRows(
      List<ArrowBatch> arrowBatches, ResultData resultData, ResultManifest resultManifest)
      throws DatabricksParsingException {
    if (arrowBatches == null) {
      throw parsingError("ARROW_BATCH response is missing arrow_batches");
    }

    long rowCount = 0;
    for (int index = 0; index < arrowBatches.size(); index++) {
      ArrowBatch arrowBatch = arrowBatches.get(index);
      if (arrowBatch == null
          || arrowBatch.getBatch() == null
          || arrowBatch.getBatch().length == 0) {
        throw parsingError("Arrow batch " + index + " is missing batch bytes");
      }
      if (arrowBatch.getRowCount() == null || arrowBatch.getRowCount() < 0) {
        throw parsingError("Arrow batch " + index + " has an invalid row_count");
      }
      try {
        rowCount = Math.addExact(rowCount, arrowBatch.getRowCount());
      } catch (ArithmeticException e) {
        throw parsingError("Arrow batch row_count total overflowed");
      }
    }

    if (resultData.getRowCount() != null && resultData.getRowCount() != rowCount) {
      throw parsingError(
          String.format(
              "Arrow batch row_count total %d does not match result row_count %d",
              rowCount, resultData.getRowCount()));
    }
    if (resultManifest.getTotalRowCount() != null
        && resultManifest.getTotalRowCount() != rowCount) {
      throw parsingError(
          String.format(
              "Arrow batch row_count total %d does not match manifest total_row_count %d",
              rowCount, resultManifest.getTotalRowCount()));
    }
    return rowCount;
  }

  private static DatabricksParsingException parsingError(String message) {
    return new DatabricksParsingException(
        message, DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
  }

  @Override
  public boolean hasNextChunk() {
    return totalRows > 0 && currentChunkIndex == -1;
  }

  @Override
  public boolean next() {
    if (!hasNextChunk()) {
      return false;
    }
    currentChunkIndex++;
    return true;
  }

  @Override
  public ArrowResultChunk getChunk() {
    return arrowResultChunk;
  }

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
}
