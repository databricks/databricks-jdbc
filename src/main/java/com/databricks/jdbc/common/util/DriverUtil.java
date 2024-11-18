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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility class for operations related to the Databricks JDBC driver.
 *
 * <p>This class provides methods for retrieving version information, setting up logging and
 * resolving metadata clients.
 */
public class DriverUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DriverUtil.class);
  private static final String VERSION = "0.9.6-oss";
  private static final String DBSQL_VERSION_SQL = "SELECT current_version().dbsql_version";
  public static final int DBSQL_MIN_MAJOR_VERSION_FOR_SEA_SUPPORT = 2024;
  public static final int DBSQL_MIN_MINOR_VERSION_FOR_SEA_SUPPORT = 30;

  /** Cached DBSQL version to avoid repeated queries to the cluster. */
  private static final AtomicReference<String> cachedDBSQLVersion = new AtomicReference<>(null);

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
    ensureUpdatedDBSQLVersionInUse(connection);
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
  static void ensureUpdatedDBSQLVersionInUse(IDatabricksConnection connection)
      throws DatabricksValidationException {
    if (connection.getConnectionContext().getClientType() != DatabricksClientType.SQL_EXEC
        || Boolean.parseBoolean(System.getProperty(IS_FAKE_SERVICE_TEST_PROP))) {
      // Check applicable only for SEA flow
      return;
    }
    String dbsqlVersion = getDBSQLVersionCached(connection);
    if (!doesDriverSupportSEA(dbsqlVersion)) {
      String errorMessage =
          String.format(
              "Unsupported DBSQL version %s. Please update your compute to use the latest DBSQL version.",
              dbsqlVersion);
      LOGGER.error(errorMessage);
      throw new DatabricksValidationException(errorMessage);
    }
  }

  private static String getDBSQLVersionCached(IDatabricksConnection connection) {
    String version = cachedDBSQLVersion.get();
    if (version != null) {
      LOGGER.debug("Using cached DBSQL Version: %s", version);
      return version;
    }

    synchronized (DriverUtil.class) {
      version = cachedDBSQLVersion.get();
      if (version != null) {
        return version;
      }

      version = queryDBSQLVersion(connection);
      cachedDBSQLVersion.set(version);
      return version;
    }
  }

  private static String queryDBSQLVersion(IDatabricksConnection connection) {
    try (ResultSet resultSet = connection.createStatement().executeQuery(DBSQL_VERSION_SQL)) {
      resultSet.next();
      String dbsqlVersion = resultSet.getString(1);
      LOGGER.debug("DBSQL Version in use: %s", dbsqlVersion);
      return dbsqlVersion;
    } catch (Exception e) {
      LOGGER.info(
          "Error retrieving DBSQL version: {%s}. Defaulting to minimum supported version.", e);
      return getDefaultDBSQLVersion();
    }
  }

  private static String getDefaultDBSQLVersion() {
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

  @VisibleForTesting
  static void clearDBSQLVersionCache() {
    cachedDBSQLVersion.set(null);
  }
}
