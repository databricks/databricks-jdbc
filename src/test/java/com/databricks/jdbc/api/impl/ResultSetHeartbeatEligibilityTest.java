package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.sdk.service.sql.StatementState;
import org.junit.jupiter.api.Test;

/**
 * Tests for heartbeat eligibility logic in DatabricksResultSet. Verifies that heartbeat is started
 * only for result sets that need it and skipped for cases where all data is already client-side or
 * the user controls polling.
 */
public class ResultSetHeartbeatEligibilityTest {

  private DatabricksResultSet createResultSet(
      StatementState state,
      StatementType statementType,
      DatabricksResultSet.ResultSetType resultSetType,
      boolean hasExecutionResult) {
    DatabricksResultSet rs = mock(DatabricksResultSet.class, CALLS_REAL_METHODS);

    // Set fields via reflection since they're private
    try {
      setField(rs, "executionStatus", new ExecutionStatus(new StatementStatus().setState(state)));
      setField(rs, "statementType", statementType);
      setField(rs, "resultSetType", resultSetType);
      setField(rs, "executionResult", hasExecutionResult ? mock(IExecutionResult.class) : null);
      setField(rs, "statementId", new StatementId("test-stmt"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return rs;
  }

  private void setField(Object obj, String fieldName, Object value) throws Exception {
    java.lang.reflect.Field field = DatabricksResultSet.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(obj, value);
  }

  // === Eligible cases ===

  @Test
  void testSeaCloudFetchIsEligible() {
    DatabricksResultSet rs =
        createResultSet(
            StatementState.SUCCEEDED,
            StatementType.QUERY,
            DatabricksResultSet.ResultSetType.SEA_ARROW_ENABLED,
            true);
    assertTrue(rs.isHeartbeatEligible(), "SEA cloud fetch should be eligible");
  }

  @Test
  void testThriftInlineIsEligible() {
    DatabricksResultSet rs =
        createResultSet(
            StatementState.SUCCEEDED,
            StatementType.QUERY,
            DatabricksResultSet.ResultSetType.THRIFT_INLINE,
            true);
    assertTrue(rs.isHeartbeatEligible(), "Thrift inline should be eligible");
  }

  @Test
  void testThriftArrowIsEligible() {
    DatabricksResultSet rs =
        createResultSet(
            StatementState.SUCCEEDED,
            StatementType.QUERY,
            DatabricksResultSet.ResultSetType.THRIFT_ARROW_ENABLED,
            true);
    assertTrue(rs.isHeartbeatEligible(), "Thrift arrow should be eligible");
  }

  @Test
  void testMetadataQueryIsEligible() {
    DatabricksResultSet rs =
        createResultSet(
            StatementState.SUCCEEDED,
            StatementType.METADATA,
            DatabricksResultSet.ResultSetType.THRIFT_INLINE,
            true);
    assertTrue(rs.isHeartbeatEligible(), "Metadata queries can have large results");
  }

  // === Ineligible cases ===

  @Test
  void testSeaInlineNotEligible() {
    DatabricksResultSet rs =
        createResultSet(
            StatementState.SUCCEEDED,
            StatementType.QUERY,
            DatabricksResultSet.ResultSetType.SEA_INLINE,
            true);
    assertFalse(rs.isHeartbeatEligible(), "SEA inline has all data in memory");
  }

  @Test
  void testDirectResultsNotEligible() {
    DatabricksResultSet rs =
        createResultSet(
            StatementState.CLOSED,
            StatementType.QUERY,
            DatabricksResultSet.ResultSetType.SEA_ARROW_ENABLED,
            true);
    assertFalse(rs.isHeartbeatEligible(), "Direct results — server already closed");
  }

  @Test
  void testThriftSucceededIsEligible() {
    // Thrift direct results arrive as SUCCEEDED (not CLOSED). isHeartbeatEligible() returns
    // true for SUCCEEDED — the heartbeat is stopped by markDirectResultsReceived() on the
    // Statement, not by the eligibility check.
    DatabricksResultSet rs =
        createResultSet(
            StatementState.SUCCEEDED,
            StatementType.QUERY,
            DatabricksResultSet.ResultSetType.THRIFT_INLINE,
            true);
    assertTrue(
        rs.isHeartbeatEligible(),
        "Thrift SUCCEEDED is eligible — markDirectResultsReceived() handles the stop");
  }

  @Test
  void testUpdateCountNotEligible() {
    DatabricksResultSet rs =
        createResultSet(
            StatementState.SUCCEEDED,
            StatementType.UPDATE,
            DatabricksResultSet.ResultSetType.UNASSIGNED,
            true);
    assertFalse(rs.isHeartbeatEligible(), "Update count has no result rows");
  }

  @Test
  void testNullExecutionResultNotEligible() {
    DatabricksResultSet rs =
        createResultSet(
            StatementState.SUCCEEDED,
            StatementType.QUERY,
            DatabricksResultSet.ResultSetType.UNASSIGNED,
            false);
    assertFalse(rs.isHeartbeatEligible(), "No execution result — nothing to fetch");
  }

  @Test
  void testAsyncPendingNotEligible() {
    DatabricksResultSet rs =
        createResultSet(
            StatementState.PENDING,
            StatementType.SQL,
            DatabricksResultSet.ResultSetType.UNASSIGNED,
            true);
    assertFalse(
        rs.isHeartbeatEligible(), "Async PENDING — user controls polling via getExecutionResult");
  }

  @Test
  void testAsyncRunningNotEligible() {
    DatabricksResultSet rs =
        createResultSet(
            StatementState.RUNNING,
            StatementType.SQL,
            DatabricksResultSet.ResultSetType.UNASSIGNED,
            true);
    assertFalse(
        rs.isHeartbeatEligible(), "Async RUNNING — user controls polling via getExecutionResult");
  }
}
