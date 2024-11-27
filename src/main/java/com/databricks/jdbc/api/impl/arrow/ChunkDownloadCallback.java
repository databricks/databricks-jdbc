package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.exception.DatabricksSQLException;

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

  /**
   * Called when new download links need to be retrieved for a chunk. This method is typically
   * invoked when the existing link for a chunk is invalid or expired.
   *
   * @param chunkIndexToDownloadLink The index of the chunk for which new links are needed
   * @throws DatabricksSQLException If there's an error retrieving the download links
   */
  void downloadLinks(long chunkIndexToDownloadLink) throws DatabricksSQLException;

  /** Returns the compression type of chunks that are to be downloaded from pre-signed URLs. */
  CompressionCodec getCompressionCodec();
}
