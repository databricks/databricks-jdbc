package com.databricks.jdbc.api.impl.arrow;

/**
 * Represents the lifecycle states of a data chunk during the download and processing pipeline. A
 * chunk transitions through these states as it moves from initial request to final consumption.
 */
public enum ChunkStatus {
  /**
   * Initial state where the chunk is awaiting URL assignment. No download URL has been fetched or
   * assigned yet.
   */
  PENDING,

  /**
   * The download URL has been successfully retrieved. Chunk is ready to begin the download process.
   */
  URL_FETCHED,

  /**
   * The chunk download operation has been initiated and is currently executing. Data transfer is in
   * progress.
   */
  DOWNLOAD_IN_PROGRESS,

  /**
   * The chunk data has been successfully downloaded and is available locally. Ready for extraction
   * and processing.
   */
  DOWNLOAD_SUCCEEDED,

  /**
   * Arrow data has been successfully processed:
   *
   * <ul>
   *   <li>Decompression completed (if compression was enabled)
   *   <li>Data converted into record batch lists
   * </ul>
   *
   * Ready for consumption by the application.
   */
  PROCESSING_SUCCEEDED,

  /** The download operation encountered an error. System will attempt to retry the download. */
  DOWNLOAD_FAILED,

  /**
   * The conversion of Arrow data into record batch lists failed. Indicates a processing error after
   * successful download.
   */
  PROCESSING_FAILED,

  /**
   * The download operation was explicitly cancelled. No further processing will occur for this
   * chunk.
   */
  CANCELLED,

  /**
   * The chunk's data has been fully consumed and its memory resources have been released back to
   * the system.
   */
  CHUNK_RELEASED,

  /**
   * Indicates that a failed download is being retried. Transitional state between DOWNLOAD_FAILED
   * and DOWNLOAD_IN_PROGRESS.
   */
  DOWNLOAD_RETRY
}
