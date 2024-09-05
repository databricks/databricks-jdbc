package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.*;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.telemetry.DatabricksMetrics;
import com.databricks.sdk.core.ProxyConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import org.apache.http.client.utils.URIBuilder;

public class DatabricksConnectionContext implements IDatabricksConnectionContext {
  private final String host;
  @VisibleForTesting final int port;
  private final String schema;
  private final String connectionURL;
  private final IDatabricksComputeResource computeResource;
  private static DatabricksMetrics metricsExporter;
  @VisibleForTesting final ImmutableMap<String, String> parameters;

  /**
   * Parses connection Url and properties into a Databricks specific connection context
   *
   * @param url Databricks server connection Url
   * @param properties connection properties
   * @return a connection context
   */
  public static IDatabricksConnectionContext parse(String url, Properties properties)
      throws DatabricksSQLException {
    if (!isValid(url)) {
      // TODO: handle exceptions properly
      throw new DatabricksParsingException("Invalid url " + url);
    }
    Matcher urlMatcher = JDBC_URL_PATTERN.matcher(url);
    if (urlMatcher.find()) {
      String hostUrlVal = urlMatcher.group(1);
      String urlMinusHost = urlMatcher.group(2);
      String[] hostAndPort = hostUrlVal.split(DatabricksJdbcConstants.PORT_DELIMITER);
      String hostValue = hostAndPort[0];
      int portValue =
          hostAndPort.length == 2
              ? Integer.parseInt(hostAndPort[1])
              : DatabricksJdbcConstants.DEFAULT_PORT;

      ImmutableMap.Builder<String, String> parametersBuilder = ImmutableMap.builder();
      String[] urlParts = urlMinusHost.split(DatabricksJdbcConstants.URL_DELIMITER);
      String schema = urlParts[0];
      if (nullOrEmptyString(schema)) {
        schema = DEFAULT_SCHEMA;
      }
      for (int urlPartIndex = 1; urlPartIndex < urlParts.length; urlPartIndex++) {
        String[] pair = urlParts[urlPartIndex].split(DatabricksJdbcConstants.PAIR_DELIMITER);
        if (pair.length == 1) {
          pair = new String[] {pair[0], ""};
        }
        if (pair[0].equalsIgnoreCase(PORT)) {
          try {
            portValue = Integer.parseInt(pair[1]);
          } catch (NumberFormatException e) {
            throw new DatabricksParsingException("Invalid port number " + pair[1]);
          }
        }
        parametersBuilder.put(pair[0].toLowerCase(), pair[1]);
      }
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        parametersBuilder.put(entry.getKey().toString().toLowerCase(), entry.getValue().toString());
      }
      DatabricksConnectionContext context =
          new DatabricksConnectionContext(
              url, hostValue, portValue, schema, parametersBuilder.build());
      metricsExporter = new DatabricksMetrics(context);
      return context;
    } else {
      // Should never reach here, since we have already checked for url validity
      throw new IllegalArgumentException("Invalid url " + "incorrect");
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port, schema, parameters);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    DatabricksConnectionContext that = (DatabricksConnectionContext) obj;
    return port == that.port
        && Objects.equals(host, that.host)
        && Objects.equals(schema, that.schema)
        && Objects.equals(parameters, that.parameters);
  }

  private DatabricksConnectionContext(
      String connectionURL,
      String host,
      int port,
      String schema,
      ImmutableMap<String, String> parameters)
      throws DatabricksSQLException {
    this.connectionURL = connectionURL;
    this.host = host;
    this.port = port;
    this.schema = schema;
    this.parameters = parameters;
    this.computeResource = buildCompute();
  }

  public static boolean isValid(String url) {
    if (!JDBC_URL_PATTERN.matcher(url).matches()) {
      return false;
    }
    return HTTP_CLUSTER_PATH_PATTERN.matcher(url).matches()
        || HTTP_WAREHOUSE_PATH_PATTERN.matcher(url).matches()
        || HTTP_ENDPOINT_PATH_PATTERN.matcher(url).matches()
        || TEST_PATH_PATTERN.matcher(url).matches()
        || BASE_PATTERN.matcher(url).matches()
        || HTTP_CLI_PATTERN.matcher(url).matches();
  }

  @Override
  public DatabricksMetrics getMetricsExporter() {
    return metricsExporter;
  }

  @Override
  public String getHostUrl() throws DatabricksParsingException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getHostUrl()");
    // Determine the schema based on the transport mode
    String schema =
        (getSSLMode() != null && getSSLMode().equals("0"))
            ? DatabricksJdbcConstants.HTTP_SCHEMA
            : DatabricksJdbcConstants.HTTPS_SCHEMA;

    schema = schema.replace("://", "");

    try {
      URIBuilder uriBuilder = new URIBuilder().setScheme(schema).setHost(this.host);

      // Conditionally add the port if it is specified
      if (port != 0) {
        uriBuilder.setPort(port);
      }

      // Build the URI and convert to string
      return uriBuilder.build().toString();
    } catch (Exception e) {
      LoggingUtil.log(LogLevel.DEBUG, "URI Building failed with exception: " + e.getMessage());
      throw new DatabricksParsingException("URI Building failed with exception: " + e.getMessage());
    }
  }

  private String getSSLMode() {
    return getParameter(DatabricksJdbcConstants.SSL);
  }

  @Override
  public IDatabricksComputeResource getComputeResource() {
    return computeResource;
  }

  private IDatabricksComputeResource buildCompute() throws DatabricksSQLException {
    String httpPath = getHttpPath();
    Matcher urlMatcher = HTTP_WAREHOUSE_PATH_PATTERN.matcher(httpPath);
    if (urlMatcher.find()) {
      return new Warehouse(urlMatcher.group(1));
    }
    urlMatcher = HTTP_ENDPOINT_PATH_PATTERN.matcher(httpPath);
    if (urlMatcher.find()) {
      return new Warehouse(urlMatcher.group(1));
    }
    urlMatcher = HTTP_CLUSTER_PATH_PATTERN.matcher(httpPath);
    if (urlMatcher.find()) {
      return new AllPurposeCluster(urlMatcher.group(1), urlMatcher.group(2));
    }
    urlMatcher = HTTP_PATH_CLI_PATTERN.matcher(httpPath);
    if (urlMatcher.find()) {
      return new AllPurposeCluster("default", "default");
    }
    // the control should never reach here, as the parsing already ensured the URL is valid
    throw new DatabricksParsingException("Invalid HTTP Path provided " + this.getHttpPath());
  }

  public String getHttpPath() {
    LoggingUtil.log(LogLevel.DEBUG, "String getHttpPath()");
    return getParameter(DatabricksJdbcConstants.HTTP_PATH);
  }

  @Override
  public String getHostForOAuth() {
    return this.host;
  }

  @Override
  public String getToken() {
    return getParameter(
        DatabricksJdbcConstants.PWD, getParameter(DatabricksJdbcConstants.PASSWORD));
  }

  @Override
  public int getAsyncExecPollInterval() {
    return getParameter(POLL_INTERVAL) == null
        ? POLL_INTERVAL_DEFAULT
        : Integer.parseInt(getParameter(DatabricksJdbcConstants.POLL_INTERVAL));
  }

  @Override
  public Boolean getDirectResultMode() {
    return getParameter(DIRECT_RESULT) == null || Objects.equals(getParameter(DIRECT_RESULT), "1");
  }

  public Cloud getCloud() throws DatabricksParsingException {
    String hostURL = getHostUrl();
    if (hostURL.contains("azuredatabricks.net")
        || hostURL.contains(".databricks.azure.cn")
        || hostURL.contains(".databricks.azure.us")) {
      return Cloud.AZURE;
    } else if (hostURL.contains(".cloud.databricks.com")) {
      return Cloud.AWS;
    }
    return Cloud.OTHER;
  }

  @Override
  public String getClientId() throws DatabricksParsingException {
    String clientId = getParameter(DatabricksJdbcConstants.CLIENT_ID);
    if (nullOrEmptyString(clientId)) {
      if (getCloud() == Cloud.AWS) {
        return DatabricksJdbcConstants.AWS_CLIENT_ID;
      } else if (getCloud() == Cloud.AZURE) {
        return DatabricksJdbcConstants.AAD_CLIENT_ID;
      }
    }
    return clientId;
  }

  @Override
  public List<String> getOAuthScopesForU2M() throws DatabricksParsingException {
    if (getCloud() == Cloud.AWS) {
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

  private String getParameter(String key, String defaultValue) {
    return this.parameters.getOrDefault(key.toLowerCase(), defaultValue);
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
  public LogLevel getLogLevel() {
    String logLevel = getParameter(DatabricksJdbcConstants.LOG_LEVEL);
    if (nullOrEmptyString(logLevel)) {
      LoggingUtil.log(
          LogLevel.DEBUG,
          "Using default log level " + DEFAULT_LOG_LEVEL + " as none was provided.");
      return DEFAULT_LOG_LEVEL;
    }
    try {
      return getLogLevel(Integer.parseInt(logLevel));
    } catch (NumberFormatException e) {
      LoggingUtil.log(LogLevel.DEBUG, "Input log level is not an integer, parsing string.");
      logLevel = logLevel.toUpperCase();
    }

    try {
      return LogLevel.valueOf(logLevel);
    } catch (Exception e) {
      LoggingUtil.log(
          LogLevel.DEBUG,
          "Using default log level " + DEFAULT_LOG_LEVEL + " as invalid level was provided.");
      return DEFAULT_LOG_LEVEL;
    }
  }

  @Override
  public String getLogPathString() {
    String parameter = getParameter(LOG_PATH);
    if (parameter != null) {
      return parameter;
    }

    String userDir = System.getProperty("user.dir");
    if (userDir != null && !userDir.isEmpty()) {
      return userDir;
    }

    // Fallback option if both LOG_PATH and user.dir are unavailable
    return System.getProperty("java.io.tmpdir", ".");
  }

  @Override
  public int getLogFileSize() {
    String parameter = getParameter(LOG_FILE_SIZE);
    return (parameter == null) ? DEFAULT_LOG_FILE_SIZE_IN_MB : Integer.parseInt(parameter);
  }

  @Override
  public int getLogFileCount() {
    String parameter = getParameter(LOG_FILE_COUNT);
    return (parameter == null) ? DEFAULT_LOG_FILE_COUNT : Integer.parseInt(parameter);
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
        : clientAgent + USER_AGENT_DELIMITER + customerUserAgent;
  }

  @Override
  public CompressionType getCompressionType() {
    // TODO: Make use of compression type
    String compressionType =
        getParameter(
            DatabricksJdbcConstants.LZ4_COMPRESSION_FLAG,
            getParameter(DatabricksJdbcConstants.COMPRESSION_FLAG));
    return CompressionType.parseCompressionType(compressionType);
  }

  @Override
  public DatabricksClientType getClientType() {
    if (computeResource instanceof AllPurposeCluster) {
      return DatabricksClientType.THRIFT;
    }
    String useThriftClient = getParameter(DatabricksJdbcConstants.USE_THRIFT_CLIENT);
    if (useThriftClient != null && useThriftClient.equals("1")) {
      return DatabricksClientType.THRIFT;
    }
    return DatabricksClientType.SQL_EXEC;
  }

  @Override
  public Boolean getUseLegacyMetadata() {
    // Defaults to use legacy metadata client
    String param = getParameter(USE_LEGACY_METADATA);
    return param != null && param.equals("1");
  }

  @Override
  public int getCloudFetchThreadPoolSize() {
    try {
      return Integer.parseInt(
          getParameter(
              CLOUD_FETCH_THREAD_POOL_SIZE, String.valueOf(CLOUD_FETCH_THREAD_POOL_SIZE_DEFAULT)));
    } catch (NumberFormatException e) {
      LoggingUtil.log(
          LogLevel.DEBUG, "Invalid thread pool size, defaulting to default thread pool size.");
      return CLOUD_FETCH_THREAD_POOL_SIZE_DEFAULT;
    }
  }

  private static boolean nullOrEmptyString(String s) {
    return s == null || s.isEmpty();
  }

  @Override
  public String getCatalog() {
    return getParameter(CATALOG, getParameter(CONN_CATALOG));
  }

  @Override
  public String getSchema() {
    return getParameter(CONN_SCHEMA, getParameter(SCHEMA));
  }

  @Override
  public Map<String, String> getSessionConfigs() {
    return this.parameters.entrySet().stream()
        .filter(
            e ->
                ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.keySet().stream()
                    .anyMatch(allowedConf -> allowedConf.toLowerCase().equals(e.getKey())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public boolean isAllPurposeCluster() {
    return this.computeResource instanceof AllPurposeCluster;
  }

  @Override
  public String getProxyHost() {
    return getParameter(DatabricksJdbcConstants.PROXY_HOST);
  }

  @Override
  public int getProxyPort() {
    return Integer.parseInt(getParameter(DatabricksJdbcConstants.PROXY_PORT));
  }

  @Override
  public String getProxyUser() {
    return getParameter(DatabricksJdbcConstants.PROXY_USER);
  }

  @Override
  public String getProxyPassword() {
    return getParameter(DatabricksJdbcConstants.PROXY_PWD);
  }

  @Override
  public Boolean getUseProxy() {
    return Objects.equals(getParameter(USE_PROXY), "1");
  }

  @Override
  public ProxyConfig.ProxyAuthType getProxyAuthType() {
    int proxyAuthTypeOrdinal = Integer.parseInt(getParameter(PROXY_AUTH, "0"));
    return ProxyConfig.ProxyAuthType.values()[proxyAuthTypeOrdinal];
  }

  @Override
  public Boolean getUseSystemProxy() {
    return Objects.equals(getParameter(USE_SYSTEM_PROXY), "1");
  }

  @Override
  public Boolean getUseCloudFetchProxy() {
    return Objects.equals(getParameter(USE_CF_PROXY), "1");
  }

  @Override
  public String getCloudFetchProxyHost() {
    return getParameter(CF_PROXY_HOST);
  }

  @Override
  public int getCloudFetchProxyPort() {
    return Integer.parseInt(getParameter(CF_PROXY_PORT));
  }

  @Override
  public String getCloudFetchProxyUser() {
    return getParameter(CF_PROXY_USER);
  }

  @Override
  public String getCloudFetchProxyPassword() {
    return getParameter(CF_PROXY_PWD);
  }

  @Override
  public ProxyConfig.ProxyAuthType getCloudFetchProxyAuthType() {
    int proxyAuthTypeOrdinal = Integer.parseInt(getParameter(CF_PROXY_AUTH, "0"));
    return ProxyConfig.ProxyAuthType.values()[proxyAuthTypeOrdinal];
  }

  @Override
  public Boolean shouldEnableArrow() {
    return Objects.equals(getParameter(ENABLE_ARROW, "1"), "1");
  }

  @Override
  public String getEndpointURL() throws DatabricksParsingException {
    return String.format("%s/%s", this.getHostUrl(), this.getHttpPath());
  }

  @VisibleForTesting
  static LogLevel getLogLevel(int level) {
    switch (level) {
      case 0:
        return LogLevel.OFF;
      case 1:
        return LogLevel.FATAL;
      case 2:
        return LogLevel.ERROR;
      case 3:
        return LogLevel.WARN;
      case 4:
        return LogLevel.INFO;
      case 5:
        return LogLevel.DEBUG;
      case 6:
        return LogLevel.TRACE;
      default:
        LoggingUtil.log(
            LogLevel.INFO,
            "Using default log level " + DEFAULT_LOG_LEVEL + " as invalid level was provided.");
        return DEFAULT_LOG_LEVEL;
    }
  }

  @Override
  public Boolean shouldRetryTemporarilyUnavailableError() {
    return Objects.equals(getParameter(TEMPORARILY_UNAVAILABLE_RETRY, "1"), "1");
  }

  @Override
  public Boolean shouldRetryRateLimitError() {
    return Objects.equals(getParameter(RATE_LIMIT_RETRY, "1"), "1");
  }

  @Override
  public int getTemporarilyUnavailableRetryTimeout() {
    return Integer.parseInt(
        getParameter(
            TEMPORARILY_UNAVAILABLE_RETRY_TIMEOUT, DEFAULT_TEMPORARILY_UNAVAILABLE_RETRY_TIMEOUT));
  }

  @Override
  public int getRateLimitRetryTimeout() {
    return Integer.parseInt(
        getParameter(RATE_LIMIT_RETRY_TIMEOUT, DEFAULT_RATE_LIMIT_RETRY_TIMEOUT));
  }

  @Override
  public int getIdleHttpConnectionExpiry() {
    return Integer.parseInt(
        getParameter(IDLE_HTTP_CONNECTION_EXPIRY, DEFAULT_IDLE_HTTP_CONNECTION_EXPIRY));
  }

  @Override
  public boolean supportManyParameters() {
    return getParameter(SUPPORT_MANY_PARAMETERS, "0").equals("1");
  }

  /** Returns whether the current test is a fake service test. */
  @Override
  public boolean isFakeServiceTest() {
    // TODO: introduce driver config/properties
    return Boolean.parseBoolean(System.getProperty(IS_FAKE_SERVICE_TEST_PROP));
  }

  @Override
  public boolean enableTelemetry() {
    return Objects.equals(getParameter(ENABLE_TELEMETRY, "0"), "1");
  }

  @Override
  public String getConnectionURL() {
    return connectionURL;
  }

  @Override
  public String getJWTKeyFile() {
    return getParameter(JWT_KEY_FILE);
  }

  @Override
  public String getKID() {
    return getParameter(JWT_KID);
  }

  @Override
  public String getJWTPassphrase() {
    return getParameter(JWT_PASS_PHRASE);
  }

  @Override
  public String getJWTAlgorithm() {
    return getParameter(JWT_ALGORITHM);
  }

  @Override
  public boolean useJWTAssertion() {
    return getParameter(USE_JWT_ASSERTION, "0").equals("1");
  }

  @Override
  public String getTokenEndpoint() {
    return getParameter(TOKEN_ENDPOINT);
  }

  @Override
  public String getAuthEndpoint() {
    return getParameter(AUTH_ENDPOINT);
  }

  @Override
  public boolean isOAuthDiscoveryModeEnabled() {
    // By default to true
    return getParameter(DISCOVERY_MODE, "1").equals("1");
  }

  @Override
  public String getOAuthDiscoveryURL() {
    return getParameter(DISCOVERY_URL);
  }

  @Override
  public String getAuthScope() {
    return getParameter(AUTH_SCOPE, ALL_APIS_SCOPE);
  }

  @Override
  public String getOAuthRefreshToken() {
    return getParameter(OAUTH_REFRESH_TOKEN);
  }
}
