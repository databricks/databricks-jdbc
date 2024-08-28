package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.sdk.core.DatabricksConfig;

public class ClientUtils {
  public static DatabricksConfig generateDatabricksConfig(
      IDatabricksConnectionContext connectionContext) throws DatabricksParsingException {
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
