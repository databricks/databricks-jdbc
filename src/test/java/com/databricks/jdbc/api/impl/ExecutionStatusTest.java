package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.sdk.service.sql.StatementState;
import org.junit.jupiter.api.Test;

class ExecutionStatusTest {

  @Test
  void cancelledStateWithNullError_hasMeaningfulMessage() {
    StatementStatus cancelledStatus =
        new StatementStatus().setState(StatementState.CANCELED).setError(null);
    ExecutionStatus status = new ExecutionStatus(cancelledStatus);
    assertNotNull(status.getErrorMessage(), "Cancelled state should have non-null error message");
    assertTrue(status.getErrorMessage().contains("cancelled"));
  }

  @Test
  void failedStateWithNullError_hasNullMessage() {
    StatementStatus failedStatus =
        new StatementStatus().setState(StatementState.FAILED).setError(null);
    ExecutionStatus status = new ExecutionStatus(failedStatus);
    assertNull(status.getErrorMessage(), "Failed state with null error should have null message");
  }

  @Test
  void succeededStateWithNullError_hasNullMessage() {
    StatementStatus succeededStatus =
        new StatementStatus().setState(StatementState.SUCCEEDED).setError(null);
    ExecutionStatus status = new ExecutionStatus(succeededStatus);
    assertNull(status.getErrorMessage());
  }
}
