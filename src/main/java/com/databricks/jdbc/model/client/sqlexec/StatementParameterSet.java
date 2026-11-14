package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.sdk.service.sql.StatementParameterListItem;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** One ordered set of native positional bindings in a parameter_sets request. */
public class StatementParameterSet {
  @JsonProperty("parameters")
  private final List<StatementParameterListItem> parameters;

  public StatementParameterSet(List<StatementParameterListItem> parameters) {
    this.parameters = parameters;
  }

  public List<StatementParameterListItem> getParameters() {
    return parameters;
  }
}
