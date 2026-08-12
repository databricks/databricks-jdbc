package com.databricks.jdbc.api.impl.arrow;

/**
 * ChunkProvider for empty results (total_chunk_count == 0). Returns no chunks and zero rows,
 * short-circuiting all downstream chunk fetching.
 */
class EmptyChunkProvider implements ChunkProvider {

  private boolean isClosed;

  @Override
  public boolean hasNextChunk() {
    return false;
  }

  @Override
  public boolean next() {
    return false;
  }

  @Override
  public AbstractArrowResultChunk getChunk() {
    return null;
  }

  @Override
  public void close() {
    isClosed = true;
  }

  @Override
  public long getRowCount() {
    return 0;
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
