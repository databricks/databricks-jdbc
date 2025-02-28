package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.common.CompressionCodec;

/**
 * Callback interface for chunk download operations. This interface defines methods that are called
 * during the chunk download process.
 */
interface ChunkDownloadCallback {
  /**
   * Called when a chunk download has been processed, regardless of the outcome. This method can be
   * used to update the state of the download manager or trigger further actions.
   *
   * @param chunkIndex The index of the chunk that has been processed
   */
  void downloadProcessed(long chunkIndex);

  /** Returns the compression type of chunks that are to be downloaded from pre-signed URLs. */
  CompressionCodec getCompressionCodec();
}
