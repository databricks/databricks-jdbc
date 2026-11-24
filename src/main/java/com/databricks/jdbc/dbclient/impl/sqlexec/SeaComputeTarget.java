package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.SESSION_PATH;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.SESSION_PATH_WITH_ID;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.STATEMENT_PATH;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.STATEMENT_PATH_WITH_ID;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.WAREHOUSE_SESSION_PATH;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.WAREHOUSE_SESSION_PATH_WITH_ID;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.WAREHOUSE_STATEMENT_PATH;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.WAREHOUSE_STATEMENT_PATH_WITH_ID;

import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.client.sqlexec.AllPurposeClusterCompute;
import com.databricks.jdbc.model.client.sqlexec.BackendCompute;

/** Centralizes SQL Execution API paths and request targeting for one compute resource. */
public final class SeaComputeTarget {
  private final IDatabricksComputeResource computeResource;
  private final String warehouseId;
  private final String orgId;
  private final String clusterId;
  private final boolean realtimeSession;

  private SeaComputeTarget(
      IDatabricksComputeResource computeResource,
      String warehouseId,
      String orgId,
      String clusterId,
      boolean realtimeSession) {
    this.computeResource = computeResource;
    this.warehouseId = warehouseId;
    this.orgId = orgId;
    this.clusterId = clusterId;
    this.realtimeSession = realtimeSession;
  }

  public static SeaComputeTarget from(IDatabricksComputeResource resource)
      throws DatabricksValidationException {
    return from(resource, false);
  }

  public static SeaComputeTarget from(ImmutableSessionInfo sessionInfo)
      throws DatabricksValidationException {
    return from(sessionInfo.computeResource(), sessionInfo.realtimeSession());
  }

  public static SeaComputeTarget from(IDatabricksComputeResource resource, boolean realtimeSession)
      throws DatabricksValidationException {
    if (resource instanceof Warehouse) {
      return new SeaComputeTarget(
          resource, ((Warehouse) resource).getWarehouseId(), null, null, realtimeSession);
    }
    if (resource instanceof AllPurposeCluster) {
      AllPurposeCluster cluster = (AllPurposeCluster) resource;
      if (cluster.getClusterId() == null || cluster.getClusterId().trim().isEmpty()) {
        throw new DatabricksValidationException("APC cluster ID must not be empty");
      }
      return new SeaComputeTarget(
          resource, null, cluster.getOrgId(), cluster.getClusterId(), false);
    }
    throw new DatabricksValidationException(
        "Unsupported SQL Execution API compute resource: "
            + (resource == null ? "null" : resource.getClass().getName()));
  }

  public boolean isAllPurposeCluster() {
    return clusterId != null;
  }

  public boolean isRealtimeSession() {
    return realtimeSession;
  }

  public boolean acceptsRealtimeHeader() {
    return !isAllPurposeCluster();
  }

  public String warehouseId() {
    return warehouseId;
  }

  public String orgId() {
    return orgId;
  }

  public String clusterId() {
    return clusterId;
  }

  public IDatabricksComputeResource computeResource() {
    return computeResource;
  }

  public BackendCompute backendCompute() {
    return isAllPurposeCluster()
        ? new BackendCompute()
            .setAllPurposeClusterCompute(new AllPurposeClusterCompute().setClusterId(clusterId))
        : null;
  }

  public String createSessionPath() {
    return isAllPurposeCluster()
        ? SESSION_PATH
        : String.format(WAREHOUSE_SESSION_PATH, warehouseId);
  }

  public String sessionPath(String sessionId) {
    return realtimeSession
        ? String.format(WAREHOUSE_SESSION_PATH_WITH_ID, warehouseId, sessionId)
        : String.format(SESSION_PATH_WITH_ID, sessionId);
  }

  public String statementCollectionPath(String sessionId) {
    return realtimeSession
        ? String.format(WAREHOUSE_STATEMENT_PATH, warehouseId, sessionId)
        : STATEMENT_PATH;
  }

  public String statementPath(String sessionId, String statementId) {
    return realtimeSession
        ? String.format(WAREHOUSE_STATEMENT_PATH_WITH_ID, warehouseId, sessionId, statementId)
        : String.format(STATEMENT_PATH_WITH_ID, statementId);
  }

  public String serializeSessionId(String sessionId) {
    if (isAllPurposeCluster()) {
      return String.format("a|%s|%s|%s", orgId, clusterId, sessionId);
    }
    String serialized = String.format("s|%s|%s", warehouseId, sessionId);
    return realtimeSession ? serialized + "|r" : serialized;
  }

  public StatementId statementId(String rawStatementId, ImmutableSessionInfo sessionInfo) {
    if (isAllPurposeCluster()) {
      return StatementId.forAllPurposeCluster(rawStatementId, (AllPurposeCluster) computeResource);
    }
    return sessionInfo == null
        ? new StatementId(rawStatementId)
        : StatementId.forCurrentSession(rawStatementId, sessionInfo);
  }
}
