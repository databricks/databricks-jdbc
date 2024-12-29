package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.exception.DatabricksParsingException;
import java.util.*;

/**
 * Manages state transitions for ArrowResultChunk. Enforces valid state transitions and provides
 * clear error messages for invalid transitions.
 */
public class ArrowResultChunkStateMachine {
  private static final Map<ChunkStatus, Set<ChunkStatus>> VALID_TRANSITIONS = new HashMap<>();

  static {
    // Initialize valid state transitions
    VALID_TRANSITIONS.put(
        ChunkStatus.PENDING,
        new HashSet<>(Arrays.asList(ChunkStatus.URL_FETCHED, ChunkStatus.CHUNK_RELEASED)));

    VALID_TRANSITIONS.put(
        ChunkStatus.URL_FETCHED,
        new HashSet<>(
            Arrays.asList(
                ChunkStatus.DOWNLOAD_SUCCEEDED,
                ChunkStatus.DOWNLOAD_FAILED,
                ChunkStatus.CANCELLED,
                ChunkStatus.CHUNK_RELEASED)));

    VALID_TRANSITIONS.put(
        ChunkStatus.DOWNLOAD_SUCCEEDED,
        new HashSet<>(
            Arrays.asList(
                ChunkStatus.PROCESSING_SUCCEEDED,
                ChunkStatus.PROCESSING_FAILED,
                ChunkStatus.CHUNK_RELEASED)));

    VALID_TRANSITIONS.put(
        ChunkStatus.PROCESSING_SUCCEEDED, new HashSet<>(List.of(ChunkStatus.CHUNK_RELEASED)));

    VALID_TRANSITIONS.put(
        ChunkStatus.DOWNLOAD_FAILED,
        new HashSet<>(Arrays.asList(ChunkStatus.DOWNLOAD_RETRY, ChunkStatus.CHUNK_RELEASED)));

    VALID_TRANSITIONS.put(
        ChunkStatus.PROCESSING_FAILED, new HashSet<>(List.of(ChunkStatus.CHUNK_RELEASED)));

    VALID_TRANSITIONS.put(
        ChunkStatus.CANCELLED, new HashSet<>(List.of(ChunkStatus.CHUNK_RELEASED)));

    VALID_TRANSITIONS.put(
        ChunkStatus.DOWNLOAD_RETRY,
        new HashSet<>(Arrays.asList(ChunkStatus.URL_FETCHED, ChunkStatus.CHUNK_RELEASED)));

    // CHUNK_RELEASED is a terminal state with no valid logical transitions
    VALID_TRANSITIONS.put(ChunkStatus.CHUNK_RELEASED, Collections.emptySet());
  }

  private ChunkStatus currentStatus;
  private final long chunkIndex;
  private final String statementId;

  public ArrowResultChunkStateMachine(
      ChunkStatus initialStatus, long chunkIndex, String statementId) {
    this.currentStatus = initialStatus;
    this.chunkIndex = chunkIndex;
    this.statementId = statementId;
  }

  /**
   * Attempts to transition to the target state.
   *
   * @param targetStatus The desired target state
   * @throws DatabricksParsingException if the transition is invalid
   */
  public synchronized void transition(ChunkStatus targetStatus) throws DatabricksParsingException {
    if (targetStatus == currentStatus) {
      return; // No transition needed
    }

    Set<ChunkStatus> validTargets = VALID_TRANSITIONS.get(currentStatus);
    if (validTargets == null || !validTargets.contains(targetStatus)) {
      throw new DatabricksParsingException(
          String.format(
              "Invalid state transition for chunk [%d] and statement [%s]: %s -> %s",
              chunkIndex, statementId, currentStatus, targetStatus));
    }

    currentStatus = targetStatus;
  }

  /**
   * Checks if a transition to the target state is valid from the current state.
   *
   * @param targetStatus The target state to check
   * @return true if the transition is valid, false otherwise
   */
  public synchronized boolean isValidTransition(ChunkStatus targetStatus) {
    Set<ChunkStatus> validTargets = VALID_TRANSITIONS.get(currentStatus);
    return validTargets != null && validTargets.contains(targetStatus);
  }

  /**
   * Returns a set of valid target states from the current state.
   *
   * @return Set of valid target states
   */
  public synchronized Set<ChunkStatus> getValidTargetStates() {
    Set<ChunkStatus> validTargets = VALID_TRANSITIONS.get(currentStatus);
    return validTargets != null
        ? Collections.unmodifiableSet(validTargets)
        : Collections.emptySet();
  }

  /**
   * Returns the current state of the chunk.
   *
   * @return current ChunkStatus
   */
  public synchronized ChunkStatus getCurrentStatus() {
    return currentStatus;
  }
}
