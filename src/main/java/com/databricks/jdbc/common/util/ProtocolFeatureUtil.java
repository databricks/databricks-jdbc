package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.model.client.thrift.generated.TProtocolVersion.*;
import static com.databricks.jdbc.model.client.thrift.generated.TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7;

/**
 * Utility class for checking Spark protocol version features. Provides methods to determine if
 * specific protocol features are supported.
 */
public final class ProtocolFeatureUtil {
  // Prevent instantiation
  private ProtocolFeatureUtil() {}

  /**
   * Checks if the given protocol version supports getting additional information in OpenSession.
   *
   * @param protocolVersion The protocol version to check
   * @return true if getInfos in OpenSession is supported, false otherwise
   */
  public static boolean supportsGetInfosInOpenSession(int protocolVersion) {
    return protocolVersion >= SPARK_CLI_SERVICE_PROTOCOL_V1.getValue();
  }

  /**
   * Checks if the given protocol version supports direct results.
   *
   * @param protocolVersion The protocol version to check
   * @return true if direct results are supported, false otherwise
   */
  public static boolean supportsDirectResults(int protocolVersion) {
    return protocolVersion >= SPARK_CLI_SERVICE_PROTOCOL_V1.getValue();
  }

  /**
   * Checks if the given protocol version supports modified hasMoreRows semantics.
   *
   * @param protocolVersion The protocol version to check
   * @return true if modified hasMoreRows semantics are supported, false otherwise
   */
  public static boolean supportsModifiedHasMoreRowsSemantics(int protocolVersion) {
    return protocolVersion >= SPARK_CLI_SERVICE_PROTOCOL_V1.getValue();
  }

  /**
   * Checks if the given protocol version supports cloud result fetching.
   *
   * @param protocolVersion The protocol version to check
   * @return true if cloud fetch is supported, false otherwise
   */
  public static boolean supportsCloudFetch(int protocolVersion) {
    return protocolVersion >= SPARK_CLI_SERVICE_PROTOCOL_V3.getValue();
  }

  /**
   * Checks if the given protocol version supports multiple catalogs in metadata operations.
   *
   * @param protocolVersion The protocol version to check
   * @return true if multiple catalogs are supported, false otherwise
   */
  public static boolean supportsMultipleCatalogs(int protocolVersion) {
    return protocolVersion >= SPARK_CLI_SERVICE_PROTOCOL_V4.getValue();
  }

  /**
   * Checks if the given protocol version supports Arrow metadata in result sets.
   *
   * @param protocolVersion The protocol version to check
   * @return true if Arrow metadata is supported, false otherwise
   */
  public static boolean supportsArrowMetadata(int protocolVersion) {
    return protocolVersion >= SPARK_CLI_SERVICE_PROTOCOL_V5.getValue();
  }

  /**
   * Checks if the given protocol version supports getting result set metadata from fetch results.
   *
   * @param protocolVersion The protocol version to check
   * @return true if getting result set metadata from fetch is supported, false otherwise
   */
  public static boolean supportsResultSetMetadataFromFetch(int protocolVersion) {
    return protocolVersion >= SPARK_CLI_SERVICE_PROTOCOL_V5.getValue();
  }

  /**
   * Checks if the given protocol version supports advanced Arrow types.
   *
   * @param protocolVersion The protocol version to check
   * @return true if advanced Arrow types are supported, false otherwise
   */
  public static boolean supportsAdvancedArrowTypes(int protocolVersion) {
    return protocolVersion >= SPARK_CLI_SERVICE_PROTOCOL_V5.getValue();
  }

  /**
   * Checks if the given protocol version supports compressed Arrow batches.
   *
   * @param protocolVersion The protocol version to check
   * @return true if compressed Arrow batches are supported, false otherwise
   */
  public static boolean supportsCompressedArrowBatches(int protocolVersion) {
    return protocolVersion >= SPARK_CLI_SERVICE_PROTOCOL_V6.getValue();
  }

  /**
   * Checks if the given protocol version supports async metadata execution.
   *
   * @param protocolVersion The protocol version to check
   * @return true if async metadata execution is supported, false otherwise
   */
  public static boolean supportsAsyncMetadataExecution(int protocolVersion) {
    return protocolVersion >= SPARK_CLI_SERVICE_PROTOCOL_V6.getValue();
  }

  /**
   * Checks if the given protocol version supports result persistence mode.
   *
   * @param protocolVersion The protocol version to check
   * @return true if result persistence mode is supported, false otherwise
   */
  public static boolean supportsResultPersistenceMode(int protocolVersion) {
    return protocolVersion >= SPARK_CLI_SERVICE_PROTOCOL_V7.getValue();
  }

  /**
   * Checks if the given protocol version supports parameterized queries.
   *
   * @param protocolVersion The protocol version to check
   * @return true if parameterized queries are supported, false otherwise
   */
  public static boolean supportsParameterizedQueries(int protocolVersion) {
    return protocolVersion >= SPARK_CLI_SERVICE_PROTOCOL_V8.getValue();
  }

  /**
   * Checks if the given protocol version supports async metadata operations.
   *
   * @param protocolVersion The protocol version to check
   * @return true if async metadata operations are supported, false otherwise
   */
  public static boolean supportsAsyncMetadataOperations(int protocolVersion) {
    return protocolVersion >= SPARK_CLI_SERVICE_PROTOCOL_V9.getValue();
  }
}
