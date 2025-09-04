package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.impl.converters.ArrowToJavaObjectConverter;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import org.apache.arrow.vector.ValueVector;

public class ArrowResultChunkIterator {
  private final AbstractArrowResultChunk resultChunk;

  // total number of record batches in the chunk
  private final int recordBatchesInChunk;

  // index of record batch in chunk
  private int recordBatchCursorInChunk;

  // total number of rows in record batch under consideration
  private int rowsInRecordBatch;

  // current row index in current record batch
  private int rowCursorInRecordBatch;

  // total number of rows read
  private int rowsReadByIterator;

  ArrowResultChunkIterator(AbstractArrowResultChunk resultChunk) {
    this.resultChunk = resultChunk;
    this.recordBatchesInChunk = resultChunk.getRecordBatchCountInChunk();
    // start before first batch
    this.recordBatchCursorInChunk = -1;
    // initialize to -1
    this.rowsInRecordBatch = -1;
    // start before first row
    this.rowCursorInRecordBatch = -1;
    // initialize rows read to 0
    this.rowsReadByIterator = 0;
  }

  /**
   * Moves iterator to the next row of the chunk. Returns false if it is at the last row in the
   * chunk.
   */
  boolean nextRow() {
    if (!hasNextRow()) {
      return false;
    }

    // Either not initialized or crossed record batch boundary
    if (rowsInRecordBatch < 0 || ++rowCursorInRecordBatch == rowsInRecordBatch) {
      // reset rowCursor to 0
      rowCursorInRecordBatch = 0;
      // Fetches number of rows in the record batch using the number of values in the first column
      // vector
      recordBatchCursorInChunk++;
      while (recordBatchCursorInChunk < recordBatchesInChunk
          && resultChunk.recordBatchList.get(recordBatchCursorInChunk).get(0).getValueCount()
              == 0) {
        recordBatchCursorInChunk++;
      }
      rowsInRecordBatch =
          resultChunk.recordBatchList.get(recordBatchCursorInChunk).get(0).getValueCount();
    }
    rowsReadByIterator++;

    return true;
  }

  /** Returns whether the next row in the chunk exists. */
  boolean hasNextRow() {
    if (rowsReadByIterator >= resultChunk.numRows) return false;
    // If there are more rows in record batch
    return (rowCursorInRecordBatch < rowsInRecordBatch - 1)
        // or there are more record batches to be processed
        || (recordBatchCursorInChunk < recordBatchesInChunk - 1);
  }

  /** Returns object in the current row at the specified columnIndex. */
  Object getColumnObjectAtCurrentRow(
      int columnIndex, ColumnInfoTypeName requiredType, String arrowMetadata, ColumnInfo columnInfo)
      throws DatabricksSQLException {
    ValueVector columnVector =
        this.resultChunk.getColumnVector(this.recordBatchCursorInChunk, columnIndex);
    return ArrowToJavaObjectConverter.convert(
        columnVector, this.rowCursorInRecordBatch, requiredType, arrowMetadata, columnInfo);
  }

  String getType(int columnIndex) {
    return this.resultChunk.getArrowMetadata().get(columnIndex);
  }
}
