package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.IStatementStatus;
import com.databricks.jdbc.api.StatementState;

/**
 * This class implements the IStatementStatus interface and provides a default implementation for
 * the methods defined in the interface. It is used to represent the status of a SQL statement
 * execution in Databricks.
 */
class StatementStatus implements IStatementStatus {
  private final StatementState state;
  private final String errorMessage;
  private final String sqlState;
  private final com.databricks.jdbc.model.core.StatementStatus sdkStatus;

  public StatementStatus(com.databricks.jdbc.model.core.StatementStatus status) {
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
  public StatementState getState() {
    return state;
  }

  com.databricks.jdbc.model.core.StatementStatus getSdkStatus() {
    return sdkStatus;
  }

  private StatementState getStateFromSdkState(com.databricks.sdk.service.sql.StatementState state) {
    if (state == null) {
      return StatementState.PENDING;
    }
    // Map the SDK statement state to the JDBC statement state
    switch (state) {
      case PENDING:
        return StatementState.PENDING;
      case RUNNING:
        return StatementState.RUNNING;
      case SUCCEEDED:
        return StatementState.SUCCEEDED;
      case FAILED:
        return StatementState.FAILED;
      case CANCELED:
        return StatementState.ABORTED;
      case CLOSED:
        return StatementState.CLOSED;
        // should never reach here
      default:
        throw new IllegalArgumentException("Unknown statement execution state: " + state);
    }
  }
}
