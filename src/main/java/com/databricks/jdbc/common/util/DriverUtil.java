package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.IS_FAKE_SERVICE_TEST_PROP;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionInternal;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Utility class for operations related to the Databricks JDBC driver.
 *
 * <p>This class provides methods for retrieving version information, setting up logging and
 * resolving metadata clients.
 */
public class DriverUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DriverUtil.class);
  public static final String DBSQL_VERSION_SQL = "SELECT current_version().dbsql_version";
  private static final String VERSION = "0.9.9-oss";
  private static final String DRIVER_NAME = "oss-jdbc";
  private static final int DBSQL_MIN_MAJOR_VERSION_FOR_SEA_SUPPORT = 2024;
  private static final int DBSQL_MIN_MINOR_VERSION_FOR_SEA_SUPPORT = 30;

  /** Cached DBSQL version mapped by HTTP path to avoid repeated queries to the cluster. */
  private static final ConcurrentMap<String, String> cachedDBSQLVersions =
      new ConcurrentHashMap<>();

  private static final String[] VERSION_PARTS = VERSION.split("[.-]");

  public static String getVersion() {
    return VERSION;
  }

  public static String getDriverName() {
    return DRIVER_NAME;
  }

  public static int getMajorVersion() {
    return Integer.parseInt(VERSION_PARTS[0]);
  }

  public static int getMinorVersion() {
    return Integer.parseInt(VERSION_PARTS[1]);
  }

  public static void resolveMetadataClient(IDatabricksConnectionInternal connection)
      throws DatabricksValidationException {
    if (connection.getConnectionContext().getUseEmptyMetadata()) {
      LOGGER.warn("Empty metadata client is being used.");
      connection.getSession().setEmptyMetadataClient();
    }
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
      throw new DatabricksSQLException(
          errMsg, e, DatabricksDriverErrorCode.LOGGING_INITIALISATION_ERROR);
    }
  }

  public static String getRootCauseMessage(Throwable e) {
    Throwable rootCause = getRootCause(e);
    return rootCause instanceof DatabricksSQLException && rootCause.getMessage() != null
        ? rootCause.getMessage()
        : e.getMessage();
  }

  /**
   * Returns whether the driver is running against fake services based on request/response stubs.
   */
  public static boolean isRunningAgainstFake() {
    return Boolean.parseBoolean(System.getProperty(IS_FAKE_SERVICE_TEST_PROP));
  }

  private static Throwable getRootCause(Throwable throwable) {
    Throwable cause;
    while ((cause = throwable.getCause()) != null && cause != throwable) {
      throwable = cause;
    }
    return throwable;
  }
}
