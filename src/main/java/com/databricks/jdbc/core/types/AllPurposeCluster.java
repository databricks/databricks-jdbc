package com.databricks.jdbc.core.types;

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
  public String toString() {
    return String.format(
        "AllPurpose cluster with clusterId {%s} and hostName {%s}", clusterId, hostName);
  }
}
