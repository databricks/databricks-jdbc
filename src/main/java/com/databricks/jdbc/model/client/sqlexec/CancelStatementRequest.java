package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.sdk.support.ToStringer;
import java.util.Objects;

public class CancelStatementRequest {
  private String statementId;

  public CancelStatementRequest() {}

  public CancelStatementRequest setStatementId(String statementId) {
    this.statementId = statementId;
    return this;
  }

  public String getStatementId() {
    return this.statementId;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      CancelStatementRequest that = (CancelStatementRequest) o;
      return Objects.equals(this.statementId, that.statementId);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(new Object[] {this.statementId});
  }

  public String toString() {
    return (new ToStringer(CancelStatementRequest.class))
        .add("statementId", this.statementId)
        .toString();
  }
}
