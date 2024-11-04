package com.databricks.jdbc.common.util;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.sql.ResultSet;

/**
 * Utility class for common operations related to the Databricks JDBC driver.
 *
 * <p>This class provides methods for retrieving version information and resolving metadata clients
 */
public class DriverUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DriverUtil.class);
  private static final String VERSION = "0.9.6-oss";
  private static final String DBSQL_VERSION_SQL = "SELECT current_version().dbsql_version";
  public static final int DBSQL_MIN_MAJOR_VERSION_FOR_SEA_SUPPORT = 2024;
  public static final int DBSQL_MIN_MINOR_VERSION_FOR_SEA_SUPPORT = 30;

  private static final String[] VERSION_PARTS = VERSION.split("[.-]");

  public static String getVersion() {
    return VERSION;
  }

  public static int getMajorVersion() {
    return Integer.parseInt(VERSION_PARTS[0]);
  }

  public static int getMinorVersion() {
    return Integer.parseInt(VERSION_PARTS[1]);
  }

  public static void resolveMetadataClient(IDatabricksConnection connection) {
    if (connection.getConnectionContext().getClientType() != DatabricksClientType.SQL_EXEC) {
      // Updated DBR version is required only in the SEA flow metadata commands
      return;
    }
    if (connection.getConnectionContext().getUseEmptyMetadata()
        || !isUpdatedDBRVersionInUse(connection)) {
      LOGGER.warn("Empty metadata client is being used.");
      connection.getSession().setEmptyMetadataClient();
    }
  }

  @VisibleForTesting
  static boolean isUpdatedDBRVersionInUse(IDatabricksConnection connection) {
    try {
      ResultSet getDBSQLVersionInfo = connection.createStatement().executeQuery(DBSQL_VERSION_SQL);
      getDBSQLVersionInfo.next();
      String dbrVersion = getDBSQLVersionInfo.getString(1);
      LOGGER.debug("DBR Version being used %s", dbrVersion);
      boolean driverSupportsSEA = doesDriverSupportSEA(dbrVersion);
      if (!driverSupportsSEA) {
        LOGGER.warn("The JDBC driver does not support the DBR in use.");
      }
      return driverSupportsSEA;
    } catch (Exception e) {
      LOGGER.info(
          "Unable to parse DBSQL version due to error {%s}. Assuming updated DBR version."
              + "Set `useEmptyMetadata` in connection parameters if it is not the case.",
          e);
      return true;
    }
  }

  private static boolean doesDriverSupportSEA(String dbsqlVersion) {
    String[] parts = dbsqlVersion.split("\\.");
    int majorVersion = Integer.parseInt(parts[0]);
    int minorVersion = Integer.parseInt(parts[1]);
    if (majorVersion == DBSQL_MIN_MAJOR_VERSION_FOR_SEA_SUPPORT) {
      return minorVersion >= DBSQL_MIN_MINOR_VERSION_FOR_SEA_SUPPORT;
    }
    return majorVersion > DBSQL_MIN_MAJOR_VERSION_FOR_SEA_SUPPORT;
  }
}
