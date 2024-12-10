package com.databricks.jdbc.api.impl.volume;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksVolumeClient;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.sql.Connection;

public class DatabricksVolumeClientFactory {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksVolumeClientFactory.class);

  public static IDatabricksVolumeClient getVolumeClient(Connection con) {
    LOGGER.debug(
        String.format(
            "Entering public static IDatabricksVolumeClient getVolumeClient with Connection con = {%s}",
            con));
    return new DatabricksUCVolumeClient(con);
  }

  public static IDatabricksVolumeClient getVolumeClient(
      IDatabricksConnectionContext connectionContext) {
    LOGGER.debug(
        String.format(
            "Entering public static IDatabricksVolumeClient getVolumeClient with IDatabricksConnectionContext connectionContext = {%s}",
            connectionContext));
    return new DBFSVolumeClient(connectionContext);
  }
}
