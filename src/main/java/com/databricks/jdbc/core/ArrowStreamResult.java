package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ChunkInfo;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.google.common.collect.ImmutableMap;

import java.sql.SQLException;

class ArrowStreamResult implements IExecutionResult {

  private final long totalRows;
  private final long totalChunks;
  private final ImmutableMap<Long, ChunkInfo> rowOffsetToChunkMap;

  private long currentRowIndex;
  private int currentChunkIndex;

  ArrowStreamResult(ResultManifest resultManifest, ResultData resultData) {
    this.totalRows = resultManifest.getTotalRowCount();
    this.totalChunks = resultManifest.getTotalChunkCount();
    this.rowOffsetToChunkMap = getRowOffsetMap(resultManifest);
    // Initialize to before first row
    this.currentRowIndex = -1;
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
  public long getCurrentRow() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean next() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
