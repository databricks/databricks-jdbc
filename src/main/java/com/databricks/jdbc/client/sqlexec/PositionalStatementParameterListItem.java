package com.databricks.jdbc.client.sqlexec;

import com.databricks.sdk.service.sql.StatementParameterListItem;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class PositionalStatementParameterListItem extends StatementParameterListItem {

  /** session-id */
  @JsonProperty("ordinal")
  private int ordinal;

  public PositionalStatementParameterListItem setOrdinal(int ordinal) {
    this.ordinal = ordinal;
    return this;
  }

  public int getOrdinal() {
    return ordinal;
  }

  public boolean equals(Object o) {
    if (o != null && o.getClass() == getClass()) {
      return super.equals(o)
          && Objects.equals(this.ordinal, ((PositionalStatementParameterListItem) o).ordinal);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), ordinal);
  }

  @Override
  public String toString() {
    return new ToStringer(StatementParameterListItem.class)
        .add("name", super.getName())
        .add("value", super.getValue())
        .add("type", super.getType())
        .toString();
  }
}
