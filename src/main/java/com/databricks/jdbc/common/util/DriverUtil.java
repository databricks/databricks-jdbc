package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.IS_FAKE_SERVICE_TEST_PROP;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionInternal;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.IOException;

/**
 * Utility class for operations related to the Databricks JDBC driver.
 *
 * <p>This class provides methods for retrieving version information, setting up logging and
 * resolving metadata clients.
 */
public class DriverUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DriverUtil.class);
  private static final String DRIVER_VERSION = "3.1.1";
  private static final String DRIVER_NAME = "oss-jdbc";
  private static final String JDBC_VERSION = "4.3";

  private static final String[] JDBC_VERSION_PARTS = JDBC_VERSION.split("[.-]");
  private static final String[] DRIVER_VERSION_PARTS = DRIVER_VERSION.split("[.-]");

  public static String getDriverVersion() {
    return DRIVER_VERSION;
  }

  public static String getDriverVersionWithoutOSSSuffix() {
    return DRIVER_VERSION.replace("-oss", "");
  }

  public static String getDriverName() {
    return DRIVER_NAME;
  }

  public static int getDriverMajorVersion() {
    return Integer.parseInt(DRIVER_VERSION_PARTS[0]);
  }

  public static int getDriverMinorVersion() {
    return Integer.parseInt(DRIVER_VERSION_PARTS[1]);
  }

  public static int getJDBCMajorVersion() {
    return Integer.parseInt(JDBC_VERSION_PARTS[0]);
  }

  public static int getJDBCMinorVersion() {
    return Integer.parseInt(JDBC_VERSION_PARTS[1]);
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
              "Error initializing the Java Util Logger (JUL) with error: %s", e.getMessage());
      LOGGER.error(e, errMsg);
      throw new DatabricksSQLException(
          errMsg, e, DatabricksDriverErrorCode.LOGGING_INITIALISATION_ERROR);
    }
  }

  /**
   * Returns whether the driver is running against fake services based on request/response stubs.
   */
  public static boolean isRunningAgainstFake() {
    return Boolean.parseBoolean(System.getProperty(IS_FAKE_SERVICE_TEST_PROP));
  }
}
