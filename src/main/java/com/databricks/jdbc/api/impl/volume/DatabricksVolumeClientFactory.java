package com.databricks.jdbc.api.impl.volume;

import com.databricks.jdbc.api.IDatabricksVolumeClient;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.sql.Connection;

/** Factory class for creating instances of {@link IDatabricksVolumeClient}. */
public class DatabricksVolumeClientFactory {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksVolumeClientFactory.class);

  /**
   * Creates an instance of the DatabricksUCVolumeClient from the given connection.
   *
   * @param con Connection
   * @return an instance of {@link IDatabricksVolumeClient}
   */
  public static IDatabricksVolumeClient getVolumeClient(Connection con) {
    LOGGER.debug(
        String.format(
            "Entering public static IDatabricksVolumeClient getVolumeClient with Connection con = {%s}",
            con));
    return new DatabricksUCVolumeClient(con);
  }

  /**
   * Creates an instance of the DBFVolumeClient from the given connectionContext.
   *
   * @param connectionContext IDatabricksConnectionContext
   * @return an instance of {@link IDatabricksVolumeClient}
   */
  public static IDatabricksVolumeClient getVolumeClient(
      IDatabricksConnectionContext connectionContext) {
    LOGGER.debug(
        String.format(
            "Entering public static IDatabricksVolumeClient getVolumeClient with IDatabricksConnectionContext connectionContext = {%s}",
            connectionContext));
    DatabricksThreadContextHolder.setConnectionContext(connectionContext);
    return new DBFSVolumeClient(connectionContext);
  }
}
