package com.databricks.jdbc.model.core;

import com.databricks.sdk.service.sql.ServiceError;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class StatementStatus {
  @JsonProperty("error")
  private ServiceError error;

  @JsonProperty("state")
  private StatementState state;

  @JsonProperty("sql_state")
  private String sqlState;

  public StatementStatus() {}

  public StatementStatus setError(ServiceError error) {
    this.error = error;
    return this;
  }

  public ServiceError getError() {
    return this.error;
  }

  public StatementStatus setState(StatementState state) {
    this.state = state;
    return this;
  }

  public StatementState getState() {
    return this.state;
  }

  public StatementStatus setSqlState(String sqlState) {
    this.sqlState = sqlState;
    return this;
  }

  public String getSqlState() {
    return this.sqlState;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      StatementStatus that = (StatementStatus) o;
      return Objects.equals(this.error, that.error)
          && Objects.equals(this.state, that.state)
          && Objects.equals(this.sqlState, that.sqlState);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(new Object[] {this.error, this.state, this.sqlState});
  }

  public String toString() {
    return (new ToStringer(StatementStatus.class))
        .add("error", this.error)
        .add("state", this.state)
        .add("sqlState", this.sqlState)
        .toString();
  }
}
