package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ColumnVector;
import com.databricks.jdbc.model.core.ColumnarData;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reads SEA {@code columnar_data} (HIVE) results without copying values into rows.
 *
 * <p>Continuation is indicated only by {@code next_chunk_index}: when present, the next chunk is
 * fetched via GetResultData with that index and the running {@code row_offset}. Absent {@code
 * next_chunk_index} ends the result. {@code total_chunk_count} is never used as a paging signal.
 */
public class ColumnarDataResult implements IExecutionResult {
  private static final class Chunk {
    final List<ColumnVector> columns;
    final int rowCount;

    Chunk(List<ColumnVector> columns, int rowCount) {
      this.columns = columns;
      this.rowCount = rowCount;
    }
  }

  private List<Chunk> chunks;
  private long totalRowCount;
  private long currentRow = -1;
  private boolean closed;

  /** Single-chunk or already-combined data with no further fetch. */
  public ColumnarDataResult(ResultManifest manifest, ResultData data)
      throws DatabricksSQLException {
    this(manifest, data, null, null);
  }

  /**
   * Reads the initial chunk and follows {@code next_chunk_index} until it is absent.
   *
   * @param statementId statement used to fetch continuations; required when the initial chunk has
   *     {@code next_chunk_index}
   * @param session session whose client fetches continuations; required with {@code statementId}
   */
  public ColumnarDataResult(
      ResultManifest manifest, ResultData data, StatementId statementId, IDatabricksSession session)
      throws DatabricksSQLException {
    chunks = new ArrayList<>();
    ResultData current = data;
    Integer expectedColumnCount = null;
    long nextRowOffset =
        current != null && current.getRowOffset() != null ? current.getRowOffset() : 0L;
    while (true) {
      Chunk chunk = parseChunk(manifest, current, expectedColumnCount);
      if (expectedColumnCount == null) {
        expectedColumnCount = chunk.columns.size();
      }
      chunks.add(chunk);
      totalRowCount += chunk.rowCount;
      nextRowOffset += chunk.rowCount;
      Long nextChunkIndex = current == null ? null : current.getNextChunkIndex();
      if (nextChunkIndex == null) {
        break;
      }
      if (session == null || statementId == null) {
        throw invalid("Multi-chunk columnar result requires a session to fetch continuation");
      }
      try {
        current =
            session
                .getDatabricksClient()
                .getResultChunksData(statementId, nextChunkIndex, nextRowOffset);
      } catch (DatabricksSQLException e) {
        throw new DatabricksSQLException(
            String.format("Failed to fetch columnar chunk %d: %s", nextChunkIndex, e.getMessage()),
            e,
            DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR);
      }
    }
  }

  private static Chunk parseChunk(
      ResultManifest manifest, ResultData data, Integer expectedColumnCount)
      throws DatabricksSQLException {
    ColumnarData columnarData = data == null ? null : data.getColumnarData();
    if (columnarData == null) {
      throw invalid("Missing columnar_data in SEA result");
    }
    List<ColumnVector> columns =
        columnarData.getColumns() == null
            ? Collections.emptyList()
            : new ArrayList<>(columnarData.getColumns());
    if (manifest != null
        && manifest.getSchema() != null
        && manifest.getSchema().getColumnCount() != null
        && manifest.getSchema().getColumnCount() != columns.size()) {
      throw invalid("Columnar result does not match manifest schema");
    }
    if (expectedColumnCount != null && columns.size() != expectedColumnCount) {
      throw invalid("Columnar chunk column count does not match earlier chunks");
    }
    int rowCount = columns.isEmpty() ? 0 : sizeOf(columns.get(0));
    for (ColumnVector column : columns) {
      if (sizeOf(column) != rowCount) {
        throw invalid("Columnar result has inconsistent vector lengths");
      }
    }
    return new Chunk(columns, rowCount);
  }

  private static int sizeOf(ColumnVector column) throws DatabricksSQLException {
    if (column == null || column.getValues() == null) {
      throw invalid("Unsupported columnar values arm");
    }
    return column.getValues().size();
  }

  private static DatabricksSQLException invalid(String message) {
    return new DatabricksSQLException(
        message,
        DatabricksDriverErrorCode.INVALID_STATE.name(),
        DatabricksDriverErrorCode.INVALID_STATE);
  }

  private Chunk chunkForCurrentRow() throws DatabricksSQLException {
    if (currentRow < 0 || currentRow >= totalRowCount) {
      throw invalid("Cursor is outside the columnar result");
    }
    long remaining = currentRow;
    for (Chunk chunk : chunks) {
      if (remaining < chunk.rowCount) {
        return chunk;
      }
      remaining -= chunk.rowCount;
    }
    throw invalid("Cursor is outside the columnar result");
  }

  private int localRowInChunk() throws DatabricksSQLException {
    long remaining = currentRow;
    for (Chunk chunk : chunks) {
      if (remaining < chunk.rowCount) {
        return (int) remaining;
      }
      remaining -= chunk.rowCount;
    }
    throw invalid("Cursor is outside the columnar result");
  }

  @Override
  public Object getObject(int columnIndex) throws DatabricksSQLException {
    if (closed) {
      throw new DatabricksSQLException(
          "Result is already closed", DatabricksDriverErrorCode.STATEMENT_CLOSED);
    }
    Chunk chunk = chunkForCurrentRow();
    if (columnIndex < 0 || columnIndex >= chunk.columns.size()) {
      throw invalid("Column index out of bounds " + columnIndex);
    }
    int localRow = localRowInChunk();
    ColumnVector column = chunk.columns.get(columnIndex);
    byte[] mask = column.getNullMask();
    int byteIndex = localRow / 8;
    if (mask != null
        && byteIndex < mask.length
        && ((mask[byteIndex] & 0xff) & (1 << (localRow % 8))) != 0) {
      return null;
    }
    return column.getValue(localRow);
  }

  @Override
  public long getCurrentRow() {
    return currentRow;
  }

  @Override
  public boolean next() {
    if (hasNext()) {
      currentRow++;
      return true;
    }
    if (!closed) {
      currentRow = totalRowCount;
    }
    return false;
  }

  @Override
  public boolean hasNext() {
    return !closed && currentRow + 1 < totalRowCount;
  }

  @Override
  public void close() {
    closed = true;
    chunks = Collections.emptyList();
  }

  @Override
  public long getRowCount() {
    return totalRowCount;
  }

  @Override
  public long getChunkCount() {
    return chunks == null ? 0 : chunks.size();
  }
}
