package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.exception.DatabricksValidationException;
import org.junit.jupiter.api.Test;

class SeaComputeTargetTest {
  @Test
  void allPurposeClusterUsesStandardPathsAndBackendCompute() throws Exception {
    SeaComputeTarget target = SeaComputeTarget.from(new AllPurposeCluster("org", "cluster"), false);

    assertTrue(target.isAllPurposeCluster());
    assertFalse(target.acceptsRealtimeHeader());
    assertEquals(PathConstants.SESSION_PATH, target.createSessionPath());
    assertEquals(PathConstants.SESSION_PATH + "session", target.sessionPath("session"));
    assertEquals(PathConstants.STATEMENT_PATH, target.statementCollectionPath("session"));
    assertEquals(
        PathConstants.STATEMENT_PATH + "statement", target.statementPath(null, "statement"));
    assertEquals("s|org|cluster|session|a", target.serializeSessionId("session"));
    assertNotNull(target.backendCompute());
    assertEquals("cluster", target.backendCompute().getAllPurposeClusterCompute().getClusterId());
  }

  @Test
  void standardWarehousePreservesStandardStatementPaths() throws Exception {
    SeaComputeTarget target = SeaComputeTarget.from(new Warehouse("warehouse"), false);

    assertFalse(target.isAllPurposeCluster());
    assertTrue(target.acceptsRealtimeHeader());
    assertEquals("/api/2.0/sql/warehouses/warehouse/sessions", target.createSessionPath());
    assertEquals(PathConstants.SESSION_PATH + "session", target.sessionPath("session"));
    assertEquals(PathConstants.STATEMENT_PATH, target.statementCollectionPath("session"));
    assertEquals(
        PathConstants.STATEMENT_PATH + "statement", target.statementPath(null, "statement"));
    assertEquals("s|warehouse|session", target.serializeSessionId("session"));
    assertNull(target.backendCompute());
  }

  @Test
  void realtimeWarehouseUsesOwnedSessionPaths() throws Exception {
    SeaComputeTarget target = SeaComputeTarget.from(new Warehouse("warehouse"), true);

    assertTrue(target.isRealtimeSession());
    assertEquals(
        "/api/2.0/sql/warehouses/warehouse/sessions/session", target.sessionPath("session"));
    assertEquals(
        "/api/2.0/sql/warehouses/warehouse/sessions/session/statements",
        target.statementCollectionPath("session"));
    assertEquals(
        "/api/2.0/sql/warehouses/warehouse/sessions/session/statements/statement",
        target.statementPath("session", "statement"));
    assertEquals("s|warehouse|session|r", target.serializeSessionId("session"));
  }

  @Test
  void rejectsMissingApcClusterId() {
    assertThrows(
        DatabricksValidationException.class,
        () -> SeaComputeTarget.from(new AllPurposeCluster("org", "")));
  }
}
