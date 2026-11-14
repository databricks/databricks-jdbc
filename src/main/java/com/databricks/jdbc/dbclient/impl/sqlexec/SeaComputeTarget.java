package com.databricks.jdbc.dbclient.impl.sqlexec;

import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.model.client.sqlexec.AllPurposeClusterCompute;
import com.databricks.jdbc.model.client.sqlexec.BackendCompute;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/** The request target and URL family for a SEA compute resource. */
public final class SeaComputeTarget {
  private static final String CLUSTER_QUERY =
      "backend_compute.all_purpose_cluster_compute.cluster_id";

  private final IDatabricksComputeResource resource;

  private SeaComputeTarget(IDatabricksComputeResource resource) {
    if (!(resource instanceof Warehouse) && !(resource instanceof AllPurposeCluster)) {
      throw new IllegalArgumentException("Unsupported SEA compute resource");
    }
    this.resource = resource;
  }

  public static SeaComputeTarget of(IDatabricksComputeResource resource) {
    return new SeaComputeTarget(resource);
  }

  public boolean isApc() {
    return resource instanceof AllPurposeCluster;
  }

  public String warehouseId() {
    return resource instanceof Warehouse ? ((Warehouse) resource).getWarehouseId() : null;
  }

  public BackendCompute backendCompute() {
    if (!isApc()) {
      return null;
    }
    return new BackendCompute()
        .setAllPurposeClusterCompute(
            new AllPurposeClusterCompute()
                .setClusterId(((AllPurposeCluster) resource).getClusterId()));
  }

  public String createSessionPath() {
    return isApc()
        ? PathConstants.SESSION_PATH
        : String.format(PathConstants.WAREHOUSE_SESSION_PATH, warehouseId());
  }

  /** Adds request query parameters once, preserving the caller's existing parameters. */
  public String withQuery(String path, Map<String, String> parameters) {
    Map<String, String> merged = new LinkedHashMap<>(parameters);
    if (isApc()) {
      merged.put(CLUSTER_QUERY, ((AllPurposeCluster) resource).getClusterId());
    }
    if (merged.isEmpty()) {
      return path;
    }
    StringBuilder url = new StringBuilder(path);
    char separator = path.contains("?") ? '&' : '?';
    for (Map.Entry<String, String> entry : merged.entrySet()) {
      url.append(separator)
          .append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8))
          .append('=')
          .append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
      separator = '&';
    }
    return url.toString();
  }
}
