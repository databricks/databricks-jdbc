package com.databricks.jdbc.driver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksConnectionContext implements IDatabricksConnectionContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksConnectionContext.class);
  private final String host;
  private final int port;

  @VisibleForTesting final ImmutableMap<String, String> parameters;

  /**
   * Parses connection Url and properties into a Databricks specific connection context
   *
   * @param url Databricks server connection Url
   * @param properties connection properties
   * @return a connection context
   */
  public static IDatabricksConnectionContext parse(String url, Properties properties) {
    if (!isValid(url)) {
      // TODO: handle exceptions properly
      throw new IllegalArgumentException("Invalid url " + url);
    }
    Matcher urlMatcher = DatabricksJdbcConstants.JDBC_URL_PATTERN.matcher(url);

    if (urlMatcher.find()) {
      String hostUrlVal = urlMatcher.group(1);
      String urlMinusHost = urlMatcher.group(2);

      String[] hostAndPort = hostUrlVal.split(DatabricksJdbcConstants.PORT_DELIMITER);
      String hostValue = hostAndPort[0];
      int portValue =
          hostAndPort.length == 2
              ? Integer.valueOf(hostAndPort[1])
              : DatabricksJdbcConstants.DEFAULT_PORT;

      ImmutableMap.Builder<String, String> parametersBuilder = ImmutableMap.builder();
      String[] urlParts = urlMinusHost.split(DatabricksJdbcConstants.URL_DELIMITER);
      for (int urlPartIndex = 1; urlPartIndex < urlParts.length; urlPartIndex++) {
        String[] pair = urlParts[urlPartIndex].split(DatabricksJdbcConstants.PAIR_DELIMITER);
        if (pair.length != 2) {
          handleInvalidUrl(url);
        }
        parametersBuilder.put(pair[0], pair[1]);
      }
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        parametersBuilder.put(entry.getKey().toString(), entry.getValue().toString());
      }
      return new DatabricksConnectionContext(hostValue, portValue, parametersBuilder.build());
    } else {
      // Should never reach here, since we have already checked for url validity
      throw new IllegalArgumentException("Invalid url " + "incorrect");
    }
  }

  private static void handleInvalidUrl(String url) {
    throw new IllegalArgumentException("Invalid url incorrect");
  }

  private DatabricksConnectionContext(
      String host, int port, ImmutableMap<String, String> parameters) {
    this.host = host;
    this.port = port;
    this.parameters = parameters;
  }

  public static boolean isValid(String url) {
    return DatabricksJdbcConstants.JDBC_URL_PATTERN.matcher(url).matches();
  }

  @Override
  public String getHostUrl() {
    LOGGER.debug("public String getHostUrl()");
    StringBuilder hostUrlBuilder =
        new StringBuilder().append(DatabricksJdbcConstants.HTTPS_SCHEMA).append(this.host);
    if (port != 0) {
      hostUrlBuilder.append(DatabricksJdbcConstants.PORT_DELIMITER).append(port);
    }
    return hostUrlBuilder.toString();
  }

  String getHttpPath() {
    LOGGER.debug("String getHttpPath()");
    return getParameter(DatabricksJdbcConstants.HTTP_PATH);
  }

  @Override
  public String getWarehouse() {
    LOGGER.debug("public String getWarehouse()");
    String httpPath = getHttpPath();
    Matcher urlMatcher = DatabricksJdbcConstants.HTTP_PATH_PATTERN.matcher(httpPath);

    String warehouseId = null;
    if (urlMatcher.find()) {
      warehouseId = urlMatcher.group(1);
    } else {
      Matcher sqlUrlMatcher = DatabricksJdbcConstants.HTTP_PATH_SQL_PATTERN.matcher(httpPath);
      if (sqlUrlMatcher.matches()) {
        warehouseId = httpPath.substring(httpPath.lastIndexOf('/') + 1);
      } else {
        throw new IllegalArgumentException("Invalid httpPath " + httpPath);
      }
    }
    return warehouseId;
  }

  @Override
  public String getToken() {
    // TODO: decide on token/password from published specs
    return getParameter(DatabricksJdbcConstants.PASSWORD);
  }

  private String getParameter(String key) {
    return this.parameters.getOrDefault(key, null);
  }
}
