package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.core.DatabricksParsingException;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.DatabricksConfig;

public class ProxyUtil {
  public static DatabricksConfig getProxyDecoratedDatabricksConfig(
      IDatabricksConnectionContext connectionContext) throws DatabricksParsingException {
    DatabricksConfig databricksConfig =
        new DatabricksConfig()
            .setHost(connectionContext.getHostUrl())
            .setToken(connectionContext.getToken())
            .setUseSystemPropertiesHttp(connectionContext.getUseSystemProxy());

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
