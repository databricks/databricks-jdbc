package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;

// Created the test helper to be in the auth package path
public class AuthTestHelper {
  public static void setupAuthMocks(
      IDatabricksConnectionContext context, ClientConfigurator clientConfigurator) {
    DatabricksAuthClientFactory.getInstance().setConfigurator(context, clientConfigurator);
  }
}
