package com.databricks.jdbc.common;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;

public class TelemetryAuthHelper {
  public static void setupAuthMocks(
      IDatabricksConnectionContext context, ClientConfigurator clientConfigurator) {
    DatabricksClientConfiguratorManager.getInstance().setConfigurator(context, clientConfigurator);
  }
}
