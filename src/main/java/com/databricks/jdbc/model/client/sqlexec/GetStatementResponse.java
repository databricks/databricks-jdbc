package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Get statement response POJO
 *
 * <p>TODO: Replace this class with the corresponding SDK implementation once it becomes available
 */
public class GetStatementResponse {
  @JsonProperty("manifest")
  private ResultManifest manifest;

  @JsonProperty("result")
  private ResultData result;

  @JsonProperty("statement_id")
  private String statementId;

  /** The status response includes execution state and if relevant, error information. */
  @JsonProperty("status")
  private StatementStatus status;

  public GetStatementResponse setManifest(ResultManifest manifest) {
    this.manifest = manifest;
    return this;
  }

  public ResultManifest getManifest() {
    return manifest;
  }

  public GetStatementResponse setResult(ResultData result) {
    this.result = result;
    return this;
  }

  public ResultData getResult() {
    return result;
  }

  public GetStatementResponse setStatementId(String statementId) {
    this.statementId = statementId;
    return this;
  }

  public String getStatementId() {
    return statementId;
  }

  public GetStatementResponse setStatus(StatementStatus status) {
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
    GetStatementResponse that = (GetStatementResponse) o;
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
    return new ToStringer(GetStatementResponse.class)
        .add("manifest", manifest)
        .add("result", result)
        .add("statementId", statementId)
        .add("status", status)
        .toString();
  }
}
