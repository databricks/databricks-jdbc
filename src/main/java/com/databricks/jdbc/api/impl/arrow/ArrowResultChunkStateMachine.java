package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.Set;

/**
 * Manages state transitions for ArrowResultChunk. Enforces valid state transitions and provides
 * clear error messages for invalid transitions.
 */
public class ArrowResultChunkStateMachine {
  private ChunkStatus currentStatus;
  private final long chunkIndex;
  private final StatementId statementId;

  public ArrowResultChunkStateMachine(
      ChunkStatus initialStatus, long chunkIndex, StatementId statementId) {
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

    if (!currentStatus.canTransitionTo(targetStatus)) {
      throw new DatabricksParsingException(
          String.format(
              "Invalid state transition for chunk [%d] and statement [%s]: %s -> %s. Valid transitions from %s are: %s",
              chunkIndex,
              statementId,
              currentStatus,
              targetStatus,
              currentStatus,
              currentStatus.getValidTransitions()),
          DatabricksDriverErrorCode.INVALID_CHUNK_STATE_TRANSITION);
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
    return currentStatus.canTransitionTo(targetStatus);
  }

  /**
   * Returns a set of valid target states from the current state.
   *
   * @return Set of valid target states
   */
  public synchronized Set<ChunkStatus> getValidTargetStates() {
    return currentStatus.getValidTransitions();
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
