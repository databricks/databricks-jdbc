package com.databricks.jdbc.dbclient;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.common.MetadataOperationType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Submitted metadata operation and the inputs needed for JDBC conversion. */
public final class MetadataAsyncHandle {
  private final StatementId statementId;
  private final MetadataAsyncRequest request;
  private final String resolvedCatalog;
  private final String[] resolvedParent;
  private final DatabricksResultSet immediateResult;
  private final List<StatementId> fallbackStatementIds = new ArrayList<>();

  public MetadataAsyncHandle(
      StatementId statementId,
      MetadataAsyncRequest request,
      String resolvedCatalog,
      String[] resolvedParent) {
    this(statementId, request, resolvedCatalog, resolvedParent, null);
  }

  public MetadataAsyncHandle(
      StatementId statementId,
      MetadataAsyncRequest request,
      String resolvedCatalog,
      String[] resolvedParent,
      DatabricksResultSet immediateResult) {
    this.statementId = statementId;
    this.request = request;
    this.resolvedCatalog = resolvedCatalog;
    this.resolvedParent = resolvedParent == null ? null : resolvedParent.clone();
    this.immediateResult = immediateResult;
  }

  public StatementId getStatementId() {
    return statementId;
  }

  public MetadataOperationType getOperation() {
    return request.getOperation();
  }

  public MetadataAsyncRequest getRequest() {
    return request;
  }

  public String getResolvedCatalog() {
    return resolvedCatalog;
  }

  public DatabricksResultSet getImmediateResult() {
    return immediateResult;
  }

  public String[] getResolvedParent() {
    return resolvedParent == null ? null : resolvedParent.clone();
  }

  public List<StatementId> getFallbackStatementIds() {
    return Collections.unmodifiableList(fallbackStatementIds);
  }

  public void addFallbackStatementId(StatementId id) {
    fallbackStatementIds.add(id);
  }
}
