package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.service.sql.StatementStatus;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Execute statement response POJO
 *
 * <p>TODO: Replace this class with the corresponding SDK implementation once it becomes available
 */
public class ExecuteStatementResponse {
  @JsonProperty("manifest")
  private ResultManifest manifest;

  @JsonProperty("result")
  private ResultData result;

  @JsonProperty("statement_id")
  private String statementId;

  @JsonProperty("status")
  private StatementStatus status;

  public ExecuteStatementResponse setManifest(ResultManifest manifest) {
    this.manifest = manifest;
    return this;
  }

  public ResultManifest getManifest() {
    return manifest;
  }

  public ExecuteStatementResponse setResult(ResultData result) {
    this.result = result;
    return this;
  }

  public ResultData getResult() {
    return result;
  }

  public ExecuteStatementResponse setStatementId(String statementId) {
    this.statementId = statementId;
    return this;
  }

  public String getStatementId() {
    return statementId;
  }

  public ExecuteStatementResponse setStatus(StatementStatus status) {
    this.status = status;
    return this;
  }

  public StatementStatus getStatus() {
    return status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExecuteStatementResponse that = (ExecuteStatementResponse) o;
    return Objects.equals(manifest, that.manifest)
        && Objects.equals(result, that.result)
        && Objects.equals(statementId, that.statementId)
        && Objects.equals(status, that.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(manifest, result, statementId, status);
  }

  @Override
  public String toString() {
    return new ToStringer(ExecuteStatementResponse.class)
        .add("manifest", manifest)
        .add("result", result)
        .add("statementId", statementId)
        .add("status", status)
        .toString();
  }
}
