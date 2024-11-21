package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.IS_FAKE_SERVICE_TEST_PROP;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.sql.ResultSet;

/**
 * Utility class for operations related to the Databricks JDBC driver.
 *
 * <p>This class provides methods for retrieving version information, setting up logging and
 * resolving metadata clients.
 */
public class DriverUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DriverUtil.class);
  private static final String VERSION = "0.9.7-oss";
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

  public static void resolveMetadataClient(IDatabricksConnection connection)
      throws DatabricksValidationException {
    if (connection.getConnectionContext().getUseEmptyMetadata()) {
      LOGGER.warn("Empty metadata client is being used.");
      connection.getSession().setEmptyMetadataClient();
    }
    ensureUpdatedDBRVersionInUse(connection);
  }

  public static void setUpLogging(IDatabricksConnectionContext connectionContext)
      throws DatabricksSQLException {
    try {
      LoggingUtil.setupLogger(
          connectionContext.getLogPathString(),
          connectionContext.getLogFileSize(),
          connectionContext.getLogFileCount(),
          connectionContext.getLogLevel());
    } catch (IOException e) {
      String errMsg =
          String.format(
              "Error initializing the Java Util Logger (JUL) with error: {%s}", e.getMessage());
      LOGGER.error(e, errMsg);
      throw new DatabricksSQLException(errMsg, e);
    }
  }

  public static String getRootCauseMessage(Throwable e) {
    Throwable rootCause = getRootCause(e);
    return rootCause instanceof DatabricksSQLException && rootCause.getMessage() != null
        ? rootCause.getMessage()
        : e.getMessage();
  }

  private static Throwable getRootCause(Throwable throwable) {
    Throwable cause;
    while ((cause = throwable.getCause()) != null && cause != throwable) {
      throwable = cause;
    }
    return throwable;
  }

  @VisibleForTesting
  static void ensureUpdatedDBRVersionInUse(IDatabricksConnection connection)
      throws DatabricksValidationException {
    if (connection.getConnectionContext().getClientType() != DatabricksClientType.SQL_EXEC
        || isRunningAgainstFake()) {
      // Check applicable only for SEA flow
      return;
    }
    String dbrVersion = getDBRVersion(connection);
    if (!doesDriverSupportSEA(dbrVersion)) {
      String errorMessage =
          String.format(
              "Unsupported DBR version %s. Please update your compute to use the latest DBR version.",
              dbrVersion);
      LOGGER.error(errorMessage);
      throw new DatabricksValidationException(errorMessage);
    }
  }

  private static String getDBRVersion(IDatabricksConnection connection) {
    try (ResultSet resultSet = connection.createStatement().executeQuery(DBSQL_VERSION_SQL)) {
      resultSet.next();
      String dbrVersion = resultSet.getString(1);
      LOGGER.debug("DBR Version in use: %s", dbrVersion);
      return dbrVersion;
    } catch (Exception e) {
      LOGGER.info(
          "Error retrieving DBR version: {%s}. Defaulting to minimum supported version.", e);
      return getDefaultDBRVersion();
    }
  }

  private static String getDefaultDBRVersion() {
    return DBSQL_MIN_MAJOR_VERSION_FOR_SEA_SUPPORT + "." + DBSQL_MIN_MINOR_VERSION_FOR_SEA_SUPPORT;
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

  public static boolean isRunningAgainstFake() {
    return Boolean.parseBoolean(System.getProperty(IS_FAKE_SERVICE_TEST_PROP));
  }
}
