package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.impl.thrift.ThriftBatchFetcher;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TBinaryColumn;
import com.databricks.jdbc.model.client.thrift.generated.TBoolColumn;
import com.databricks.jdbc.model.client.thrift.generated.TByteColumn;
import com.databricks.jdbc.model.client.thrift.generated.TColumn;
import com.databricks.jdbc.model.client.thrift.generated.TDoubleColumn;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TI16Column;
import com.databricks.jdbc.model.client.thrift.generated.TI32Column;
import com.databricks.jdbc.model.client.thrift.generated.TI64Column;
import com.databricks.jdbc.model.client.thrift.generated.TRowSet;
import com.databricks.jdbc.model.client.thrift.generated.TStringColumn;
import com.databricks.jdbc.model.core.ColumnVector;
import com.databricks.jdbc.model.core.ColumnarData;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Adapts paged SEA columnar results to the existing Thrift columnar streaming pipeline.
 *
 * <p>Only the API continuation is SEA-specific. Once fetched, every page is represented as the same
 * {@link TFetchResultsResp} consumed by Thrift columnar results.
 */
final class SeaColumnarBatchFetcher implements ThriftBatchFetcher {
  private final ResultManifest manifest;
  private final StatementId statementId;
  private final IDatabricksSession session;
  private final TFetchResultsResp initialResponse;

  private Long nextChunkIndex;
  private long nextRowOffset;
  private long totalRows;
  private volatile boolean closed;

  SeaColumnarBatchFetcher(
      ResultData initialPage,
      ResultManifest manifest,
      StatementId statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
    this.manifest = manifest;
    this.statementId = statementId;
    this.session = session;
    this.initialResponse = adaptPage(initialPage, 0, 0);
  }

  TFetchResultsResp getInitialResponse() {
    return initialResponse;
  }

  @Override
  public TFetchResultsResp fetchNextBatch() throws SQLException {
    if (closed) {
      throw invalid("SEA columnar batch fetcher is closed");
    }
    if (nextChunkIndex == null) {
      throw invalid("SEA columnar result has no continuation page");
    }
    long chunkIndex = nextChunkIndex;
    return adaptPage(
        session.getDatabricksClient().getResultChunksData(statementId, chunkIndex, nextRowOffset),
        chunkIndex,
        nextRowOffset);
  }

  @Override
  public void close() {
    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  private TFetchResultsResp adaptPage(
      ResultData page, long expectedChunkIndex, long expectedRowOffset)
      throws DatabricksSQLException {
    if (page == null || page.getColumnarData() == null) {
      throw invalid("Missing columnar_data in SEA result page " + expectedChunkIndex);
    }
    if (page.getChunkIndex() != null && page.getChunkIndex() != expectedChunkIndex) {
      throw invalid(
          String.format(
              "SEA columnar page chunk_index %d does not match requested chunk %d",
              page.getChunkIndex(), expectedChunkIndex));
    }
    if (page.getRowOffset() != null && page.getRowOffset() != expectedRowOffset) {
      throw invalid(
          String.format(
              "SEA columnar page row_offset %d does not match requested row offset %d",
              page.getRowOffset(), expectedRowOffset));
    }
    if (page.getNextChunkIndex() != null && page.getNextChunkIndex() <= expectedChunkIndex) {
      throw invalid(
          String.format(
              "SEA columnar page %d returned non-increasing next_chunk_index %d",
              expectedChunkIndex, page.getNextChunkIndex()));
    }

    TRowSet rowSet = toThriftRowSet(page.getColumnarData());
    ColumnarRowView rowView = new ColumnarRowView(rowSet);
    validateSchema(rowView);
    long rowCount = rowView.getRowCount();
    if (page.getRowCount() != null && page.getRowCount() != rowCount) {
      throw invalid(
          String.format(
              "SEA columnar page row_count %d does not match decoded row count %d",
              page.getRowCount(), rowCount));
    }

    try {
      totalRows = Math.addExact(totalRows, rowCount);
      nextRowOffset = Math.addExact(expectedRowOffset, rowCount);
    } catch (ArithmeticException e) {
      throw invalid("SEA columnar row count overflowed");
    }
    nextChunkIndex = page.getNextChunkIndex();
    if (nextChunkIndex == null
        && manifest.getTotalRowCount() != null
        && manifest.getTotalRowCount() != totalRows) {
      throw invalid(
          String.format(
              "SEA columnar row count %d does not match manifest total_row_count %d",
              totalRows, manifest.getTotalRowCount()));
    }
    return new TFetchResultsResp().setResults(rowSet).setHasMoreRows(nextChunkIndex != null);
  }

  private void validateSchema(ColumnarRowView rowView) throws DatabricksSQLException {
    if (manifest.getSchema() != null
        && manifest.getSchema().getColumnCount() != null
        && manifest.getSchema().getColumnCount() != rowView.getColumnCount()) {
      throw invalid("Columnar result does not match manifest schema");
    }
  }

  private static TRowSet toThriftRowSet(ColumnarData columnarData) throws DatabricksSQLException {
    List<ColumnVector> vectors =
        columnarData.getColumns() == null ? Collections.emptyList() : columnarData.getColumns();
    List<TColumn> columns = new ArrayList<>(vectors.size());
    Integer rowCount = null;
    for (ColumnVector vector : vectors) {
      int vectorSize = columnSize(vector);
      if (rowCount != null && rowCount != vectorSize) {
        throw invalid("Columnar result has inconsistent vector lengths");
      }
      rowCount = vectorSize;
      columns.add(toThriftColumn(vector));
    }
    return new TRowSet().setColumns(columns);
  }

  private static int columnSize(ColumnVector vector) throws DatabricksSQLException {
    if (vector == null || vector.getValues() == null) {
      throw invalid("Unsupported columnar values arm");
    }
    return vector.getValues().size();
  }

  private static TColumn toThriftColumn(ColumnVector vector) throws DatabricksSQLException {
    byte[] nulls = vector.getNullMask();
    if (vector.getBoolValues() != null) {
      TBoolColumn column = new TBoolColumn().setValues(vector.getBoolValues().getValues());
      setNulls(column, nulls);
      return TColumn.boolVal(column);
    }
    if (vector.getByteValues() != null) {
      List<Byte> values = new ArrayList<>(vector.getByteValues().getValues().size());
      for (Integer value : vector.getByteValues().getValues()) {
        values.add(value.byteValue());
      }
      TByteColumn column = new TByteColumn().setValues(values);
      setNulls(column, nulls);
      return TColumn.byteVal(column);
    }
    if (vector.getI16Values() != null) {
      List<Short> values = new ArrayList<>(vector.getI16Values().getValues().size());
      for (Short value : vector.getI16Values().getValues()) {
        values.add(value);
      }
      TI16Column column = new TI16Column().setValues(values);
      setNulls(column, nulls);
      return TColumn.i16Val(column);
    }
    if (vector.getI32Values() != null) {
      TI32Column column = new TI32Column().setValues(vector.getI32Values().getValues());
      setNulls(column, nulls);
      return TColumn.i32Val(column);
    }
    if (vector.getI64Values() != null) {
      TI64Column column = new TI64Column().setValues(vector.getI64Values().getValues());
      setNulls(column, nulls);
      return TColumn.i64Val(column);
    }
    if (vector.getDoubleValues() != null) {
      TDoubleColumn column = new TDoubleColumn().setValues(vector.getDoubleValues().getValues());
      setNulls(column, nulls);
      return TColumn.doubleVal(column);
    }
    if (vector.getStringValues() != null) {
      TStringColumn column = new TStringColumn().setValues(vector.getStringValues().getValues());
      setNulls(column, nulls);
      return TColumn.stringVal(column);
    }
    if (vector.getBinaryValues() != null) {
      List<ByteBuffer> values = new ArrayList<>(vector.getBinaryValues().getValues().size());
      for (byte[] value : vector.getBinaryValues().getValues()) {
        values.add(ByteBuffer.wrap(value));
      }
      TBinaryColumn column = new TBinaryColumn().setValues(values);
      setNulls(column, nulls);
      return TColumn.binaryVal(column);
    }
    throw invalid("Unsupported columnar values arm");
  }

  private static void setNulls(TBoolColumn column, byte[] nulls) {
    if (nulls != null) column.setNulls(nulls);
  }

  private static void setNulls(TByteColumn column, byte[] nulls) {
    if (nulls != null) column.setNulls(nulls);
  }

  private static void setNulls(TI16Column column, byte[] nulls) {
    if (nulls != null) column.setNulls(nulls);
  }

  private static void setNulls(TI32Column column, byte[] nulls) {
    if (nulls != null) column.setNulls(nulls);
  }

  private static void setNulls(TI64Column column, byte[] nulls) {
    if (nulls != null) column.setNulls(nulls);
  }

  private static void setNulls(TDoubleColumn column, byte[] nulls) {
    if (nulls != null) column.setNulls(nulls);
  }

  private static void setNulls(TStringColumn column, byte[] nulls) {
    if (nulls != null) column.setNulls(nulls);
  }

  private static void setNulls(TBinaryColumn column, byte[] nulls) {
    if (nulls != null) column.setNulls(nulls);
  }

  private static DatabricksSQLException invalid(String message) {
    return new DatabricksSQLException(
        message,
        DatabricksDriverErrorCode.INVALID_STATE.name(),
        DatabricksDriverErrorCode.INVALID_STATE);
  }
}
