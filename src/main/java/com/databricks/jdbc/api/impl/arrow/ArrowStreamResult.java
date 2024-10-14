package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.util.DatabricksThriftUtil.getTypeFromTypeDesc;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.IExecutionResult;
import com.databricks.jdbc.api.impl.converters.ArrowToJavaObjectConverter;
import com.databricks.jdbc.common.CompressionType;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TColumnDesc;
import com.databricks.jdbc.model.client.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.model.client.thrift.generated.TRowSet;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;

public class ArrowStreamResult implements IExecutionResult {

  private final ChunkProvider chunkProvider;
  private long currentRowIndex = -1;
  private boolean isClosed;
  private ArrowResultChunk.ArrowResultChunkIterator chunkIterator;
  private List<ColumnInfo> columnInfos;

  public ArrowStreamResult(
      ResultManifest resultManifest,
      ResultData resultData,
      String statementId,
      IDatabricksSession session)
      throws DatabricksParsingException {
    this(
        resultManifest,
        resultData,
        statementId,
        session,
        DatabricksHttpClient.getInstance(session.getConnectionContext()));
  }

  @VisibleForTesting
  ArrowStreamResult(
      ResultManifest resultManifest,
      ResultData resultData,
      String statementId,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient)
      throws DatabricksParsingException {
    this.chunkProvider =
        new RemoteChunkProvider(
            statementId,
            resultManifest,
            resultData,
            session,
            httpClient,
            session.getConnectionContext().getCloudFetchThreadPoolSize());
    this.columnInfos =
        resultManifest.getSchema().getColumnCount() == 0
            ? new ArrayList<>()
            : new ArrayList<>(resultManifest.getSchema().getColumns());
  }

  public ArrowStreamResult(
      TGetResultSetMetadataResp resultManifest,
      TRowSet resultData,
      boolean isInlineArrow,
      String parentStatementId,
      IDatabricksSession session)
      throws DatabricksParsingException {
    this(
        resultManifest,
        resultData,
        isInlineArrow,
        parentStatementId,
        session,
        DatabricksHttpClient.getInstance(session.getConnectionContext()));
  }

  @VisibleForTesting
  ArrowStreamResult(
      TGetResultSetMetadataResp resultManifest,
      TRowSet resultData,
      boolean isInlineArrow,
      String statementId,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient)
      throws DatabricksParsingException {
    setColumnInfo(resultManifest);
    if (isInlineArrow) {
      this.chunkProvider =
          new InlineChunkProvider(resultData.getArrowBatches(), resultManifest, statementId);
    } else {
      CompressionType compressionType = CompressionType.getCompressionMapping(resultManifest);
      this.chunkProvider =
          new RemoteChunkProvider(
              statementId,
              resultData,
              session,
              httpClient,
              session.getConnectionContext().getCloudFetchThreadPoolSize(),
              compressionType);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Object getObject(int columnIndex) throws DatabricksSQLException {
    ColumnInfoTypeName requiredType = columnInfos.get(columnIndex).getTypeName();
    Object unconvertedObject = chunkIterator.getColumnObjectAtCurrentRow(columnIndex);
    return ArrowToJavaObjectConverter.convert(unconvertedObject, requiredType);
  }

  /** {@inheritDoc} */
  @Override
  public long getCurrentRow() {
    return currentRowIndex;
  }

  /** {@inheritDoc} */
  @Override
  public boolean next() throws DatabricksSQLException {
    if (!hasNext()) {
      return false;
    }

    currentRowIndex++;
    if (chunkIterator == null || !chunkIterator.hasNextRow()) {
      chunkProvider.next();
      chunkIterator = chunkProvider.getChunk().getChunkIterator();
    }

    return chunkIterator.nextRow();
  }

  /** {@inheritDoc} */
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
    return chunkProvider.hasNextChunk();
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    isClosed = true;
    chunkProvider.close();
  }

  private void setColumnInfo(TGetResultSetMetadataResp resultManifest) {
    columnInfos = new ArrayList<>();
    if (resultManifest.getSchema() == null) {
      return;
    }
    for (TColumnDesc columnInfo : resultManifest.getSchema().getColumns()) {
      columnInfos.add(new ColumnInfo().setTypeName(getTypeFromTypeDesc(columnInfo.getTypeDesc())));
    }
  }
}
