package com.databricks.jdbc.model.client.sqlexec;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** Backend compute target for a SQL Execution API request. */
public class BackendCompute {
  @JsonProperty("all_purpose_cluster_compute")
  private AllPurposeClusterCompute allPurposeClusterCompute;

  public AllPurposeClusterCompute getAllPurposeClusterCompute() {
    return allPurposeClusterCompute;
  }

  public BackendCompute setAllPurposeClusterCompute(
      AllPurposeClusterCompute allPurposeClusterCompute) {
    this.allPurposeClusterCompute = allPurposeClusterCompute;
    return this;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof BackendCompute
        && Objects.equals(
            allPurposeClusterCompute, ((BackendCompute) other).allPurposeClusterCompute);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(allPurposeClusterCompute);
  }
}
