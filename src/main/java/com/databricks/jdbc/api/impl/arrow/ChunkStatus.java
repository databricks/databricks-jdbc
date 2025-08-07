package com.databricks.jdbc.api.impl.arrow;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

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
  DOWNLOAD_RETRY;

  private static final Map<ChunkStatus, Set<ChunkStatus>> VALID_TRANSITIONS =
      new EnumMap<>(ChunkStatus.class);

  // Initialize valid state transitions
  static {
    VALID_TRANSITIONS.put(PENDING, Set.of(URL_FETCHED, CHUNK_RELEASED, DOWNLOAD_FAILED));
    VALID_TRANSITIONS.put(
        URL_FETCHED, Set.of(DOWNLOAD_SUCCEEDED, DOWNLOAD_FAILED, CANCELLED, CHUNK_RELEASED));
    VALID_TRANSITIONS.put(
        DOWNLOAD_SUCCEEDED, Set.of(PROCESSING_SUCCEEDED, PROCESSING_FAILED, CHUNK_RELEASED));
    VALID_TRANSITIONS.put(PROCESSING_SUCCEEDED, Set.of(CHUNK_RELEASED));
    VALID_TRANSITIONS.put(DOWNLOAD_FAILED, Set.of(DOWNLOAD_RETRY, CHUNK_RELEASED));
    VALID_TRANSITIONS.put(PROCESSING_FAILED, Set.of(CHUNK_RELEASED));
    VALID_TRANSITIONS.put(CANCELLED, Set.of(CHUNK_RELEASED));
    VALID_TRANSITIONS.put(CHUNK_RELEASED, Collections.emptySet());
    VALID_TRANSITIONS.put(
        DOWNLOAD_RETRY, Set.of(URL_FETCHED, DOWNLOAD_SUCCEEDED, DOWNLOAD_FAILED, CHUNK_RELEASED));
  }

  /**
   * Returns the set of valid target states from this state.
   *
   * @return Set of valid target states
   */
  public Set<ChunkStatus> getValidTransitions() {
    return VALID_TRANSITIONS.getOrDefault(this, Collections.emptySet());
  }

  /**
   * Checks if a transition to the target state is valid from this state.
   *
   * @param targetStatus The target state to check
   * @return true if the transition is valid, false otherwise
   */
  public boolean canTransitionTo(ChunkStatus targetStatus) {
    return getValidTransitions().contains(targetStatus);
  }
}
