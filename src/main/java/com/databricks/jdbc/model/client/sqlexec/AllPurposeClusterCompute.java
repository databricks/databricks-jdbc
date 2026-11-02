package com.databricks.jdbc.model.client.sqlexec;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** Wire model for an all-purpose-cluster target in SQL Execution API requests. */
public class AllPurposeClusterCompute {
  @JsonProperty("cluster_id")
  private String clusterId;

  public String getClusterId() {
    return clusterId;
  }

  public AllPurposeClusterCompute setClusterId(String clusterId) {
    this.clusterId = clusterId;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AllPurposeClusterCompute that = (AllPurposeClusterCompute) o;
    return Objects.equals(clusterId, that.clusterId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clusterId);
  }
}
