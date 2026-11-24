package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.impl.ComplexDataTypeParser;
import com.databricks.jdbc.api.impl.converters.ArrowToJavaObjectConverter;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.ValueVector;

public class ArrowResultChunkIterator {
  private final AbstractArrowResultChunk resultChunk;
  private final boolean[] timeColumns;
  private final Map<Integer, ComplexDataTypeParser[]> complexParsers = new HashMap<>();

  // total number of record batches in the chunk
  private final int recordBatchesInChunk;

  // index of record batch in chunk
  private int recordBatchCursorInChunk;

  // total number of rows in record batch under consideration
  private int rowsInRecordBatch;

  // current row index in current record batch
  private int rowCursorInRecordBatch;

  // last row index of the current record batch that is within the chunk's row count, so moving
  // within a batch needs a single comparison
  private int lastReadableRowInRecordBatch;

  // number of rows read before the current record batch
  private long rowsReadBeforeRecordBatch;

  ArrowResultChunkIterator(AbstractArrowResultChunk resultChunk) {
    this.resultChunk = resultChunk;
    List<String> metadata = resultChunk.getArrowMetadata();
    this.timeColumns = new boolean[metadata == null ? 0 : metadata.size()];
    for (int index = 0; index < timeColumns.length; index++) {
      timeColumns[index] = DatabricksTypeUtil.isTimeType(metadata.get(index));
    }
    this.recordBatchesInChunk = resultChunk.getRecordBatchCountInChunk();
    // start before first batch
    this.recordBatchCursorInChunk = -1;
    // initialize to -1
    this.rowsInRecordBatch = -1;
    // start before first row
    this.rowCursorInRecordBatch = -1;
    this.lastReadableRowInRecordBatch = -1;
    this.rowsReadBeforeRecordBatch = 0;
  }

  /**
   * Moves iterator to the next row of the chunk. Returns false if it is at the last row in the
   * chunk.
   */
  boolean nextRow() {
    if (rowCursorInRecordBatch < lastReadableRowInRecordBatch) {
      rowCursorInRecordBatch++;
      return true;
    }
    return nextRowInNextRecordBatch();
  }

  private boolean nextRowInNextRecordBatch() {
    if (!hasNextRow()) {
      return false;
    }
    rowsReadBeforeRecordBatch += rowCursorInRecordBatch + 1;
    // reset rowCursor to 0
    rowCursorInRecordBatch = 0;
    // Fetches number of rows in the record batch using the number of values in the first column
    // vector
    recordBatchCursorInChunk++;
    while (recordBatchCursorInChunk < recordBatchesInChunk
        && resultChunk.recordBatchList.get(recordBatchCursorInChunk).get(0).getValueCount() == 0) {
      recordBatchCursorInChunk++;
    }
    rowsInRecordBatch =
        resultChunk.recordBatchList.get(recordBatchCursorInChunk).get(0).getValueCount();
    lastReadableRowInRecordBatch =
        (int) Math.min(rowsInRecordBatch, resultChunk.numRows - rowsReadBeforeRecordBatch) - 1;
    return true;
  }

  /** Returns whether the next row in the chunk exists. */
  boolean hasNextRow() {
    if (rowCursorInRecordBatch < lastReadableRowInRecordBatch) return true;
    if (rowsReadBeforeRecordBatch + rowCursorInRecordBatch + 1 >= resultChunk.numRows) {
      return false;
    }
    // If there are more rows in record batch
    return (rowCursorInRecordBatch < rowsInRecordBatch - 1)
        // or there are more record batches to be processed
        || (recordBatchCursorInChunk < recordBatchesInChunk - 1);
  }

  /** Returns object in the current row at the specified columnIndex. */
  Object getColumnObjectAtCurrentRow(
      int columnIndex, ColumnInfoTypeName requiredType, String arrowMetadata, ColumnInfo columnInfo)
      throws DatabricksSQLException {
    return getColumnObjectAtCurrentRow(columnIndex, requiredType, arrowMetadata, columnInfo, true);
  }

  Object getColumnObjectAtCurrentRow(
      int columnIndex,
      ColumnInfoTypeName requiredType,
      String arrowMetadata,
      ColumnInfo columnInfo,
      boolean geoSpatialSupportEnabled)
      throws DatabricksSQLException {
    ValueVector columnVector =
        this.resultChunk.getColumnVector(this.recordBatchCursorInChunk, columnIndex);
    if ((requiredType == null || requiredType == ColumnInfoTypeName.STRING)
        && columnIndex < timeColumns.length
        && timeColumns[columnIndex]) {
      requiredType = ColumnInfoTypeName.TIME;
    }
    return ArrowToJavaObjectConverter.convertWithResolvedType(
        columnVector,
        this.rowCursorInRecordBatch,
        requiredType,
        arrowMetadata,
        columnInfo,
        geoSpatialSupportEnabled,
        requiredType == ColumnInfoTypeName.ARRAY
                || requiredType == ColumnInfoTypeName.MAP
                || requiredType == ColumnInfoTypeName.STRUCT
            ? getComplexParser(columnIndex, geoSpatialSupportEnabled)
            : null);
  }

  ComplexDataTypeParser getComplexParser(int columnIndex, boolean geoSpatialSupportEnabled) {
    ComplexDataTypeParser[] columnParsers = complexParsers.get(columnIndex);
    if (columnParsers == null) {
      columnParsers = new ComplexDataTypeParser[2];
      complexParsers.put(columnIndex, columnParsers);
    }
    int flagIndex = geoSpatialSupportEnabled ? 1 : 0;
    ComplexDataTypeParser parser = columnParsers[flagIndex];
    if (parser == null) {
      ValueVector vector = resultChunk.getColumnVector(recordBatchCursorInChunk, columnIndex);
      parser =
          ArrowToJavaObjectConverter.parserFor(
              vector, getType(columnIndex), geoSpatialSupportEnabled);
      columnParsers[flagIndex] = parser;
    }
    return parser;
  }

  /**
   * Returns true if the value at columnIndex can be read with {@link
   * #getJavaTimeObjectAtCurrentRow}.
   */
  boolean hasJavaTimeVector(int columnIndex) {
    return ArrowToJavaObjectConverter.isJavaTimeVector(
        this.resultChunk.getColumnVector(this.recordBatchCursorInChunk, columnIndex));
  }

  /**
   * Returns the DATE or TIMESTAMP value in the current row at the specified columnIndex, read
   * straight from the vector. See {@link ArrowToJavaObjectConverter#convertToJavaTime}.
   */
  Object getJavaTimeObjectAtCurrentRow(int columnIndex) throws DatabricksSQLException {
    ValueVector columnVector =
        this.resultChunk.getColumnVector(this.recordBatchCursorInChunk, columnIndex);
    return ArrowToJavaObjectConverter.convertToJavaTime(columnVector, this.rowCursorInRecordBatch);
  }

  String getType(int columnIndex) {
    return this.resultChunk.getArrowMetadata().get(columnIndex);
  }
}
