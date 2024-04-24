package com.databricks.jdbc.core;

import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.getTypeFromTypeDesc;

import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.client.impl.thrift.generated.TColumnDesc;
import com.databricks.jdbc.client.impl.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.client.impl.thrift.generated.TRowSet;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.ResultManifest;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class ArrowStreamResult implements IExecutionResult {

  private IDatabricksSession session;
  private ChunkDownloader chunkDownloader;
  private ChunkExtractor chunkExtractor;

  private long currentRowIndex;

  private final boolean isInlineArrow;

  private boolean isClosed;

  private ArrowResultChunk.ArrowResultChunkIterator chunkIterator;

  List<ColumnInfo> columnInfos;

  ArrowStreamResult(
      ResultManifest resultManifest,
      ResultData resultData,
      String statementId,
      IDatabricksSession session) {
    this(
        resultManifest,
        new ChunkDownloader(statementId, resultManifest, resultData, session),
        session);
  }

  ArrowStreamResult(TGetResultSetMetadataResp resultManifest, TRowSet resultData)
      throws DatabricksParsingException {
    this.chunkDownloader = null;
    setColumnInfo(resultManifest);
    this.currentRowIndex = -1;
    this.isClosed = false;
    this.isInlineArrow = true;
    this.chunkIterator = null;
    this.chunkExtractor = new ChunkExtractor(resultData.getArrowBatches(), resultManifest);
  }

  private void setColumnInfo(TGetResultSetMetadataResp resultManifest) {
    this.columnInfos = new ArrayList<>();
    if (resultManifest.getSchema() == null) {
      return;
    }
    for (TColumnDesc columnInfo : resultManifest.getSchema().getColumns()) {
      this.columnInfos.add(
          new ColumnInfo().setTypeName(getTypeFromTypeDesc(columnInfo.getTypeDesc())));
    }
  }

  @VisibleForTesting
  ArrowStreamResult(
      ResultManifest resultManifest,
      ResultData resultData,
      String statementId,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient) {
    this(
        resultManifest,
        new ChunkDownloader(statementId, resultManifest, resultData, session, httpClient),
        session);
  }

  private ArrowStreamResult(
      ResultManifest resultManifest, ChunkDownloader chunkDownloader, IDatabricksSession session) {
    this.session = session;
    this.isInlineArrow = false;
    this.chunkDownloader = chunkDownloader;
    this.columnInfos =
        resultManifest.getSchema().getColumnCount() == 0
            ? new ArrayList<>()
            : new ArrayList<>(resultManifest.getSchema().getColumns());
    this.currentRowIndex = -1;
    this.isClosed = false;
    this.chunkIterator = null;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    // we have two types:
    // 1. Required type via the metadata
    // 2. Interpreted type while reading from the arrow file into the record batches
    // We need to convert the interpreted type into the required type before returning the object
    ColumnInfoTypeName requiredType = columnInfos.get(columnIndex).getTypeName();
    Object unconvertedObject = this.chunkIterator.getColumnObjectAtCurrentRow(columnIndex);
    return ArrowToJavaObjectConverter.convert(unconvertedObject, requiredType);
  }

  @Override
  public long getCurrentRow() {
    return this.currentRowIndex;
  }

  @Override
  public boolean next() throws DatabricksSQLException {
    if (!hasNext()) {
      return false;
    }
    this.currentRowIndex++;
    if (isInlineArrow) {
      if (chunkIterator == null) {
        this.chunkIterator = this.chunkExtractor.next().getChunkIterator();
      }
      return chunkIterator.nextRow();
    }
    // Either this is first chunk or we are crossing chunk boundary
    if (this.chunkIterator == null || !this.chunkIterator.hasNextRow()) {
      this.chunkDownloader.next();
      this.chunkIterator = this.chunkDownloader.getChunk().getChunkIterator();
      return chunkIterator.nextRow();
    }
    // Traversing within a chunk
    return this.chunkIterator.nextRow();
  }

  @Override
  public boolean hasNext() {
    if (isClosed) {
      return false;
    }

    // Check if there are any more rows available in the current chunk
    if (chunkIterator != null && chunkIterator.hasNextRow()) {
      return true;
    }

    // For inline arrow, check if the chunk extractor has more chunks
    // Otherwise, check the chunk downloader
    return isInlineArrow ? this.chunkExtractor.hasNext() : this.chunkDownloader.hasNextChunk();
  }

  @Override
  public void close() {
    this.isClosed = true;
    if (isInlineArrow) {
      this.chunkExtractor.releaseChunk();
    } else {
      this.chunkDownloader.releaseAllChunks();
    }
  }
}
