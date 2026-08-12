package com.databricks.jdbc.model.client.sqlexec;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** Wire model for the SQL Execution API backend-compute discriminator. */
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
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BackendCompute that = (BackendCompute) o;
    return Objects.equals(allPurposeClusterCompute, that.allPurposeClusterCompute);
  }

  @Override
  public int hashCode() {
    return Objects.hash(allPurposeClusterCompute);
  }
}
