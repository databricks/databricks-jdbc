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
    java.util.HashSet<ChunkStatus> set;
    set = new java.util.HashSet<ChunkStatus>();
    set.add(URL_FETCHED);
    set.add(CHUNK_RELEASED);
    set.add(DOWNLOAD_FAILED);
    VALID_TRANSITIONS.put(PENDING, java.util.Collections.unmodifiableSet(set));

    set = new java.util.HashSet<ChunkStatus>();
    set.add(DOWNLOAD_SUCCEEDED);
    set.add(DOWNLOAD_FAILED);
    set.add(CANCELLED);
    set.add(CHUNK_RELEASED);
    VALID_TRANSITIONS.put(URL_FETCHED, java.util.Collections.unmodifiableSet(set));

    set = new java.util.HashSet<ChunkStatus>();
    set.add(PROCESSING_SUCCEEDED);
    set.add(PROCESSING_FAILED);
    set.add(CHUNK_RELEASED);
    VALID_TRANSITIONS.put(DOWNLOAD_SUCCEEDED, java.util.Collections.unmodifiableSet(set));

    set = new java.util.HashSet<ChunkStatus>();
    set.add(CHUNK_RELEASED);
    VALID_TRANSITIONS.put(PROCESSING_SUCCEEDED, java.util.Collections.unmodifiableSet(set));

    set = new java.util.HashSet<ChunkStatus>();
    set.add(DOWNLOAD_RETRY);
    set.add(CHUNK_RELEASED);
    VALID_TRANSITIONS.put(DOWNLOAD_FAILED, java.util.Collections.unmodifiableSet(set));

    set = new java.util.HashSet<ChunkStatus>();
    set.add(CHUNK_RELEASED);
    VALID_TRANSITIONS.put(PROCESSING_FAILED, java.util.Collections.unmodifiableSet(set));

    set = new java.util.HashSet<ChunkStatus>();
    set.add(CHUNK_RELEASED);
    VALID_TRANSITIONS.put(CANCELLED, java.util.Collections.unmodifiableSet(set));

    VALID_TRANSITIONS.put(CHUNK_RELEASED, java.util.Collections.<ChunkStatus>emptySet());

    set = new java.util.HashSet<ChunkStatus>();
    set.add(URL_FETCHED);
    set.add(DOWNLOAD_SUCCEEDED);
    set.add(DOWNLOAD_FAILED);
    set.add(CHUNK_RELEASED);
    VALID_TRANSITIONS.put(DOWNLOAD_RETRY, java.util.Collections.unmodifiableSet(set));
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
