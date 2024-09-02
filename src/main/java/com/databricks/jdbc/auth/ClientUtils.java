package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.sdk.core.DatabricksConfig;

public class ClientUtils {
  public static DatabricksConfig generateDatabricksConfig(
      IDatabricksConnectionContext connectionContext) {
    DatabricksConfig databricksConfig =
        new DatabricksConfig().setUseSystemPropertiesHttp(connectionContext.getUseSystemProxy());
    // Setup proxy settings
    if (connectionContext.getUseProxy()) {
      databricksConfig
          .setProxyHost(connectionContext.getProxyHost())
          .setProxyPort(connectionContext.getProxyPort());
    }
    databricksConfig
        .setProxyAuthType(connectionContext.getProxyAuthType())
        .setProxyUsername(connectionContext.getProxyUser())
        .setProxyPassword(connectionContext.getProxyPassword());
    return databricksConfig;
  }
}
