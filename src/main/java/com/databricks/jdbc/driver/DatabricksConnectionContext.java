package com.databricks.jdbc.driver;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.*;

import com.databricks.jdbc.client.DatabricksClientType;
import com.databricks.jdbc.core.types.CompressionType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

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
    Matcher urlMatcher = JDBC_URL_PATTERN.matcher(url);
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
        parametersBuilder.put(pair[0].toLowerCase(), pair[1]);
      }
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        parametersBuilder.put(entry.getKey().toString().toLowerCase(), entry.getValue().toString());
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
    return JDBC_URL_PATTERN.matcher(url).matches();
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
  public String getHostForOAuth() {
    return this.host;
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
    return getParameter(DatabricksJdbcConstants.PWD) == null
        ? getParameter(DatabricksJdbcConstants.PASSWORD)
        : getParameter(DatabricksJdbcConstants.PWD);
  }

  public String getCloud() {
    String hostURL = getHostUrl();
    if (hostURL.contains(".azuredatabricks.net")
        || hostURL.contains(".databricks.azure.cn")
        || hostURL.contains(".databricks.azure.us")) {
      return "AAD";
    } else if (hostURL.contains(".cloud.databricks.com")) {
      return "AWS";
    }
    return "OTHER";
  }

  @Override
  public String getClientId() {
    String clientId = getParameter(DatabricksJdbcConstants.CLIENT_ID);
    if (nullOrEmptyString(clientId)) {
      if (getCloud().equals("AWS")) {
        return DatabricksJdbcConstants.AWS_CLIENT_ID;
      } else if (getCloud().equals("AAD")) {
        return DatabricksJdbcConstants.AAD_CLIENT_ID;
      }
    }
    return clientId;
  }

  @Override
  public List<String> getOAuthScopesForU2M() {
    if (getCloud().equals("AWS")) {
      return Arrays.asList(
          DatabricksJdbcConstants.SQL_SCOPE, DatabricksJdbcConstants.OFFLINE_ACCESS_SCOPE);
    } else {
      // Default scope is already being set for Azure in databricks-sdk.
      return null;
    }
  }

  @Override
  public String getClientSecret() {
    return getParameter(DatabricksJdbcConstants.CLIENT_SECRET);
  }

  private String getParameter(String key) {
    return this.parameters.getOrDefault(key.toLowerCase(), null);
  }

  @Override
  public AuthFlow getAuthFlow() {
    String authFlow = getParameter(DatabricksJdbcConstants.AUTH_FLOW);
    if (nullOrEmptyString(authFlow)) return AuthFlow.TOKEN_PASSTHROUGH;
    return AuthFlow.values()[Integer.parseInt(authFlow)];
  }

  @Override
  public AuthMech getAuthMech() {
    String authMech = getParameter(DatabricksJdbcConstants.AUTH_MECH);
    return AuthMech.parseAuthMech(authMech);
  }

  @Override
  public String getLogLevelString() {
    String logLevel = getParameter(DatabricksJdbcConstants.LOG_LEVEL);
    if (nullOrEmptyString(logLevel)) {
      LOGGER.debug("No logLevel given in the input, defaulting to info.");
      return DEFAULT_LOG_LEVEL;
    }
    logLevel = logLevel.toUpperCase();
    try {
      Level.valueOf(logLevel);
    } catch (Exception e) {
      LOGGER.debug("Invalid logLevel given in the input, defaulting to info.");
      return DEFAULT_LOG_LEVEL;
    }
    return logLevel;
  }

  @Override
  public String getLogPathString() {
    return getParameter(DatabricksJdbcConstants.LOG_PATH);
  }

  @Override
  public String getClientUserAgent() {
    String customerUserAgent = getParameter(DatabricksJdbcConstants.USER_AGENT_ENTRY);
    String clientAgent =
        getClientType().equals(DatabricksClientType.SQL_EXEC)
            ? DatabricksJdbcConstants.USER_AGENT_SEA_CLIENT
            : DatabricksJdbcConstants.USER_AGENT_THRIFT_CLIENT;
    return nullOrEmptyString(customerUserAgent)
        ? clientAgent
        : clientAgent + " " + customerUserAgent;
  }

  // TODO: Make use of compression type
  @Override
  public CompressionType getCompressionType() {
    String compressionType =
        Optional.ofNullable(getParameter(DatabricksJdbcConstants.LZ4_COMPRESSION_FLAG))
            .orElse(getParameter(DatabricksJdbcConstants.COMPRESSION_FLAG));
    return CompressionType.parseCompressionType(compressionType);
  }

  private DatabricksClientType getClientType() {
    // TODO: decide on client type from parsed JDBC Url
    return DatabricksClientType.SQL_EXEC;
  }

  private static boolean nullOrEmptyString(String s) {
    return s == null || s.isEmpty();
  }

  @Override
  public String getCatalog() {
    return getParameter(DatabricksJdbcConstants.CONN_CATALOG);
  }

  @Override
  public String getSchema() {
    return getParameter(DatabricksJdbcConstants.CONN_SCHEMA);
  }

  @Override
  public Map<String, String> getSessionConfigs() {
    return this.parameters.entrySet().stream()
        .filter(
            e ->
                ALLOWED_SESSION_CONFIGS.stream()
                    .map(String::toLowerCase)
                    .anyMatch(allowedConf -> allowedConf.equals(e.getKey())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
