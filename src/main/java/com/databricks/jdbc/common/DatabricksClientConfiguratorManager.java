package com.databricks.jdbc.common;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksSSLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;

public class DatabricksClientConfiguratorManager {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksClientConfiguratorManager.class);
  private static final DatabricksClientConfiguratorManager INSTANCE =
      new DatabricksClientConfiguratorManager();
  private final ConfiguratorFactory configuratorFactory;
  private final ConcurrentHashMap<String, ClientConfigurator> instances = new ConcurrentHashMap<>();

  private DatabricksClientConfiguratorManager() {
    this(ClientConfigurator::new);
  }

  @VisibleForTesting
  DatabricksClientConfiguratorManager(ConfiguratorFactory configuratorFactory) {
    this.configuratorFactory = configuratorFactory;
  }

  @FunctionalInterface
  interface ConfiguratorFactory {
    ClientConfigurator create(IDatabricksConnectionContext context)
        throws DatabricksSSLException, DatabricksValidationException;
  }

  public ClientConfigurator getConfigurator(IDatabricksConnectionContext context) {
    try {
      return instances.computeIfAbsent(
          context.getConnectionUuid(), ignored -> createConfigurator(context));
    } catch (DatabricksDriverException ex) {
      throw ex;
    } catch (Exception ex) {
      throw authConfigurationException(context, ex);
    }
  }

  private ClientConfigurator createConfigurator(IDatabricksConnectionContext context) {
    try {
      return configuratorFactory.create(context);
    } catch (DatabricksDriverException e) {
      throw e;
    } catch (DatabricksSSLException e) {
      String message =
          String.format("client configurator failed due to SSL error: %s", e.getMessage());
      LOGGER.error(e, message);
      throw new DatabricksDriverException(message, e, DatabricksDriverErrorCode.AUTH_ERROR);
    } catch (DatabricksValidationException e) {
      String message =
          String.format("client configurator failed due to validation error: %s", e.getMessage());
      LOGGER.error(e, message);
      throw new DatabricksDriverException(
          message, e, DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    } catch (Exception e) {
      throw authConfigurationException(context, e);
    }
  }

  private DatabricksDriverException authConfigurationException(
      IDatabricksConnectionContext context, Throwable ex) {
    String message =
        String.format(
            "Unexpected error while configuring databricks auth client: %s, with connection context %s",
            ex.getMessage(), context);
    LOGGER.error(ex, message);
    return new DatabricksDriverException(message, ex, DatabricksDriverErrorCode.AUTH_ERROR);
  }

  /**
   * Returns the client configurator if it exists, otherwise returns null. This is is indetended to
   * be used only for telemetry clients to avoid infinite recursion.
   *
   * @param context the connection context
   * @return the client configurator if it exists, otherwise null
   */
  public ClientConfigurator getConfiguratorOnlyIfExists(IDatabricksConnectionContext context) {
    return instances.get(context.getConnectionUuid());
  }

  @VisibleForTesting
  void setConfigurator(
      IDatabricksConnectionContext context, ClientConfigurator clientConfigurator) {
    instances.put(context.getConnectionUuid(), clientConfigurator);
  }

  public static DatabricksClientConfiguratorManager getInstance() {
    return INSTANCE;
  }

  public void removeInstance(IDatabricksConnectionContext context) {
    ClientConfigurator removed = instances.remove(context.getConnectionUuid());
    if (removed != null) {
      removed.close();
    }
  }
}
