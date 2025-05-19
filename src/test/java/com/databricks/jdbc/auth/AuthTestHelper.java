package com.databricks.jdbc.auth;

import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.sdk.core.DatabricksConfig;

public class AuthTestHelper {
  public static void setupAuthMocks(
      IDatabricksConnectionContext context,
      ClientConfigurator clientConfigurator,
      DatabricksConfig databricksConfig) {
    when(clientConfigurator.getDatabricksConfig()).thenReturn(databricksConfig);
    DatabricksAuthClientFactory.getInstance().setConfigurator(context, clientConfigurator);
  }
}
