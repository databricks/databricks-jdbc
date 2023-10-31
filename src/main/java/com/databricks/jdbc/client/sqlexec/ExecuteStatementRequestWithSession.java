package com.databricks.jdbc.client.sqlexec;

import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class ExecuteStatementRequestWithSession extends ExecuteStatementRequest {

  /** session-id */
  @JsonProperty("session_id")
  private String sessionId;

  public ExecuteStatementRequestWithSession setSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  public String getSessionId() {
    return sessionId;
  }

  public boolean equals(Object o) {
    if (o != null && o.getClass() == getClass()) {
      return super.equals(o)
          && Objects.equals(this.sessionId, ((ExecuteStatementRequestWithSession) o).sessionId);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), sessionId);
  }

  @Override
  public String toString() {
    return new ToStringer(ExecuteStatementRequest.class)
        .add("byteLimit", super.getByteLimit())
        .add("catalog", super.getCatalog())
        .add("disposition", super.getDisposition())
        .add("format", super.getFormat())
        .add("onWaitTimeout", super.getOnWaitTimeout())
        .add("schema", super.getSchema())
        .add("sessionId", sessionId)
        .add("statement", super.getStatement())
        .add("waitTimeout", super.getWaitTimeout())
        .add("warehouseId", super.getWarehouseId())
        .toString();
  }
}
