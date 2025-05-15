package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.ExecutionState;
import com.databricks.jdbc.api.IExecutionStatus;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.sdk.service.sql.StatementState;

/**
 * This class implements the IStatementStatus interface and provides a default implementation for
 * the methods defined in the interface. It is used to represent the status of a SQL statement
 * execution in Databricks.
 */
class ExecutionStatus implements IExecutionStatus {
  private final ExecutionState state;
  private final String errorMessage;
  private final String sqlState;
  private final StatementStatus sdkStatus;

  public ExecutionStatus(StatementStatus status) {
    this.state = getStateFromSdkState(status.getState());
    this.errorMessage = status.getError() != null ? status.getError().getMessage() : null;
    this.sqlState = status.getSqlState();
    this.sdkStatus = status;
  }

  @Override
  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public String getSqlState() {
    return sqlState;
  }

  @Override
  public ExecutionState getExecutionState() {
    return state;
  }

  StatementStatus getSdkStatus() {
    return sdkStatus;
  }

  private ExecutionState getStateFromSdkState(StatementState state) {
    if (state == null) {
      return ExecutionState.PENDING;
    }
    // Map the SDK statement state to the JDBC statement state
    switch (state) {
      case PENDING:
        return ExecutionState.PENDING;
      case RUNNING:
        return ExecutionState.RUNNING;
      case SUCCEEDED:
        return ExecutionState.SUCCEEDED;
      case FAILED:
        return ExecutionState.FAILED;
      case CANCELED:
        return ExecutionState.ABORTED;
      case CLOSED:
        return ExecutionState.CLOSED;
        // should never reach here
      default:
        throw new IllegalArgumentException("Unknown statement execution state: " + state);
    }
  }
}
