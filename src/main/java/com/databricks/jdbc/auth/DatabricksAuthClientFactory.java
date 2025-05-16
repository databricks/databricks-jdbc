package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.concurrent.ConcurrentHashMap;

public class DatabricksAuthClientFactory {
  private static final DatabricksAuthClientFactory INSTANCE = new DatabricksAuthClientFactory();
  private final ConcurrentHashMap<String, ClientConfigurator> instances = new ConcurrentHashMap<>();

  private DatabricksAuthClientFactory() {
    // Private constructor to prevent instantiation
  }

  public ClientConfigurator getConfigurator(IDatabricksConnectionContext context) {
    if (!instances.contains(context.getConnectionUuid())) {
      try {
        instances.put(context.getConnectionUuid(), new ClientConfigurator(context));
      } catch (DatabricksHttpException e) {
        String message =
            String.format(
                "Failed to configure databricks auth client: %s, with connection context %s",
                e, context);
        throw new DatabricksDriverException(message, DatabricksDriverErrorCode.AUTH_ERROR);
      }
    }
    return instances.get(context.getConnectionUuid());
  }

  public static DatabricksAuthClientFactory getInstance() {
    return INSTANCE;
  }

  public void removeInstance(IDatabricksConnectionContext context) {
    instances.remove(context.getConnectionUuid());
  }
}
