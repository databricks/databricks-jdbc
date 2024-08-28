package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.jdbc.common.CompressionType;
import com.databricks.sdk.service.sql.Disposition;
import com.databricks.sdk.service.sql.ExecuteStatementRequestOnWaitTimeout;
import com.databricks.sdk.service.sql.Format;
import com.databricks.sdk.service.sql.StatementParameterListItem;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.Objects;

/* TODO: This file is picked from databricks-sdk-java. Remove this once arclight changes are included in API-SPEC */
public class ExecuteStatementRequest {
  @JsonProperty("statement")
  private String statement;

  @JsonProperty("warehouse_id")
  private String warehouseId;

  @JsonProperty("row_limit")
  private Long rowLimit;

  @JsonProperty("session_id")
  private String sessionId;

  @JsonProperty("disposition")
  private Disposition disposition;

  @JsonProperty("format")
  private Format format;

  @JsonProperty("on_wait_timeout")
  private ExecuteStatementRequestOnWaitTimeout onWaitTimeout;

  @JsonProperty("wait_timeout")
  private String waitTimeout;

  @JsonProperty("parameters")
  private Collection<StatementParameterListItem> parameters;

  @JsonProperty("compression_codec")
  private CompressionType compressionType;

  public String getStatement() {
    return statement;
  }

  public String getWarehouseId() {
    return warehouseId;
  }

  public Long getRowLimit() {
    return rowLimit;
  }

  public String getSessionId() {
    return sessionId;
  }

  public Disposition getDisposition() {
    return disposition;
  }

  public Format getFormat() {
    return format;
  }

  public ExecuteStatementRequestOnWaitTimeout getOnWaitTimeout() {
    return onWaitTimeout;
  }

  public String getWaitTimeout() {
    return waitTimeout;
  }

  public Collection<StatementParameterListItem> getParameters() {
    return parameters;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  // Setters
  public ExecuteStatementRequest setStatement(String statement) {
    this.statement = statement;
    return this;
  }

  public ExecuteStatementRequest setCompressionType(CompressionType compressionType) {
    this.compressionType = compressionType;
    return this;
  }

  public ExecuteStatementRequest setWarehouseId(String warehouseId) {
    this.warehouseId = warehouseId;
    return this;
  }

  public ExecuteStatementRequest setRowLimit(long rowLimit) {
    this.rowLimit = rowLimit;
    return this;
  }

  public ExecuteStatementRequest setSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  public ExecuteStatementRequest setDisposition(Disposition disposition) {
    this.disposition = disposition;
    return this;
  }

  public ExecuteStatementRequest setFormat(Format format) {
    this.format = format;
    return this;
  }

  public ExecuteStatementRequest setOnWaitTimeout(
      ExecuteStatementRequestOnWaitTimeout onWaitTimeout) {
    this.onWaitTimeout = onWaitTimeout;
    return this;
  }

  public ExecuteStatementRequest setWaitTimeout(String waitTimeout) {
    this.waitTimeout = waitTimeout;
    return this;
  }

  public ExecuteStatementRequest setParameters(Collection<StatementParameterListItem> parameters) {
    this.parameters = parameters;
    return this;
  }

  @Override
  public String toString() {
    return new ToStringer(ExecuteStatementRequest.class)
        .add("disposition", disposition)
        .add("format", format)
        .add("onWaitTimeout", onWaitTimeout)
        .add("parameters", parameters)
        .add("statement", statement)
        .add("sessionId", sessionId)
        .add("waitTimeout", waitTimeout)
        .add("warehouseId", warehouseId)
        .add("rowLimit", rowLimit)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        disposition,
        format,
        onWaitTimeout,
        parameters,
        rowLimit,
        statement,
        waitTimeout,
        warehouseId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    ExecuteStatementRequest that = (ExecuteStatementRequest) o;
    return Objects.equals(disposition, that.disposition)
        && Objects.equals(format, that.format)
        && Objects.equals(onWaitTimeout, that.onWaitTimeout)
        && Objects.equals(parameters, that.parameters)
        && Objects.equals(rowLimit, that.rowLimit)
        && Objects.equals(statement, that.statement)
        && Objects.equals(waitTimeout, that.waitTimeout)
        && Objects.equals(sessionId, that.sessionId)
        && Objects.equals(warehouseId, that.warehouseId);
  }
}
