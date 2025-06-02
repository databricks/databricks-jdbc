package com.databricks.jdbc.common;

import java.util.Objects;

public class AllPurposeCluster implements IDatabricksComputeResource {
  private final String clusterId;
  private final String orgId;

  public AllPurposeCluster(String orgId, String clusterId) {
    this.clusterId = clusterId;
    this.orgId = orgId;
  }

  public String getClusterId() {
    return this.clusterId;
  }

  public String getOrgId() {
    return this.orgId;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj.getClass() != this.getClass()) {
      return false;
    }
    return Objects.equals(((AllPurposeCluster) obj).clusterId, this.clusterId)
        && Objects.equals(((AllPurposeCluster) obj).orgId, this.orgId);
  }

  @Override
  public String toString() {
    return String.format("AllPurpose cluster with clusterId {%s} and orgId {%s}", clusterId, orgId);
  }

  @Override
  public String getWorkspaceId() {
    return this.orgId;
  }

  @Override
  public String getUniqueIdentifier() {
    return String.format("%s/%s", this.orgId, this.clusterId);
  }
}
