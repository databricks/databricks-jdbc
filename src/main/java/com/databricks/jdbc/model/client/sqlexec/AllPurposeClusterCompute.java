package com.databricks.jdbc.model.client.sqlexec;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** Cluster target for an all-purpose SQL Execution API request. */
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
  public boolean equals(Object other) {
    return other instanceof AllPurposeClusterCompute
        && Objects.equals(clusterId, ((AllPurposeClusterCompute) other).clusterId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(clusterId);
  }
}
