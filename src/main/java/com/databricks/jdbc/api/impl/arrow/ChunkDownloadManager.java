package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.exception.DatabricksSQLException;

/**
 * Interface for managing the downloading of Arrow data chunks from Databricks servers. This
 * interface defines methods for handling the asynchronous download of result chunks and their
 * associated download links.
 */
interface ChunkDownloadManager {
  /**
   * Downloads the next set of chunks based on the current memory constraints and chunk
   * availability. This method manages the download process by respecting:
   *
   * <ul>
   *   <li>The total number of chunks allowed in memory
   *   <li>Whether the provider has been closed
   *   <li>The availability of more chunks to download
   * </ul>
   *
   * @throws DatabricksSQLException If there's an error during the chunk download process
   */
  void downloadNextChunks() throws DatabricksSQLException;

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
