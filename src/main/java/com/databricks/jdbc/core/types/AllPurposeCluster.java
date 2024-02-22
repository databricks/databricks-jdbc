package com.databricks.jdbc.core.types;

import java.util.Objects;

public class AllPurposeCluster implements ComputeResource {
  private final String clusterId;
  private final String hostName;

  public AllPurposeCluster(String clusterId, String hostName) {
    this.clusterId = clusterId;
    this.hostName = hostName;
  }

  public String getClusterId() {
    return this.clusterId;
  }

  public String getHostName() {
    return this.hostName;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj.getClass() != this.getClass()) {
      return false;
    }
    return Objects.equals(((AllPurposeCluster) obj).clusterId, this.clusterId)
        && Objects.equals(((AllPurposeCluster) obj).clusterId, this.hostName);
  }

  @Override
  public String toString() {
    return String.format(
        "AllPurpose cluster with clusterId {%s} and hostName {%s}", clusterId, hostName);
  }
}
