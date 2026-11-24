package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.sdk.service.sql.StatementParameterListItem;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/** Driver-owned SEA model for one ordered statement parameter set. */
public class StatementParameterSet {

  @JsonProperty("parameters")
  private Collection<StatementParameterListItem> parameters;

  public Collection<StatementParameterListItem> getParameters() {
    return parameters;
  }

  public StatementParameterSet setParameters(Collection<StatementParameterListItem> parameters) {
    this.parameters = parameters == null ? null : List.copyOf(parameters);
    return this;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof StatementParameterSet)) {
      return false;
    }
    StatementParameterSet that = (StatementParameterSet) other;
    return Objects.equals(parameters, that.parameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parameters);
  }

  @Override
  public String toString() {
    return "StatementParameterSet{parameterCount="
        + (parameters == null ? 0 : parameters.size())
        + '}';
  }
}
