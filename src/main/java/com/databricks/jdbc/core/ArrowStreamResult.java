package com.databricks.jdbc.core;

import com.databricks.client.jdbc42.internal.apache.arrow.memory.RootAllocator;
import com.databricks.sdk.service.sql.ChunkInfo;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.google.common.collect.ImmutableMap;

import java.sql.SQLException;

class ArrowStreamResult implements IExecutionResult {

  private final long totalRows;
  private final long totalChunks;

  private final IDatabricksSession session;
  private final ImmutableMap<Long, ChunkInfo> rowOffsetToChunkMap;
  private final ChunkDownloader chunkDownloader;

  private int currentRowIndex;
  private int currentChunkIndex;

  private final RootAllocator rootAllocator;
  private static final int rootAllocatorLimit = Integer.MAX_VALUE; // change if required

  ArrowStreamResult(ResultManifest resultManifest, ResultData resultData) {
    this.totalRows = resultManifest.getTotalRowCount();
    this.totalChunks = resultManifest.getTotalChunkCount();
    this.rowOffsetToChunkMap = getRowOffsetMap(resultManifest);
    this.session = session;
    this.chunkDownloader = new ChunkDownloader(statementId, resultManifest, resultData, session);
    this.rootAllocator = new RootAllocator(rootAllocatorLimit);
  }

  private static ImmutableMap<Long, ChunkInfo> getRowOffsetMap(ResultManifest resultManifest) {
    ImmutableMap.Builder<Long, ChunkInfo> rowOffsetMapBuilder = ImmutableMap.builder();
    for (ChunkInfo chunk : resultManifest.getChunks()) {
      rowOffsetMapBuilder.put(chunk.getRowOffset(), chunk);
    }
    return rowOffsetMapBuilder.build();
  }
  
  @Override
  public Object getObject(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getCurrentRow() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean next() {
    throw new UnsupportedOperationException("Not implemented");
  }
  @Override
  public boolean hasNext() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
