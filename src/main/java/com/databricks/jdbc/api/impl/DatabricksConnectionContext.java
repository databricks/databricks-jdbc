package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;
import static com.databricks.jdbc.common.util.UserAgentManager.USER_AGENT_SEA_CLIENT;
import static com.databricks.jdbc.common.util.UserAgentManager.USER_AGENT_THRIFT_CLIENT;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.*;
import com.databricks.jdbc.common.util.ValidationUtil;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.DatabricksEnvironment;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.utils.Cloud;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import org.apache.http.client.utils.URIBuilder;

public class DatabricksConnectionContext implements IDatabricksConnectionContext {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksConnectionContext.class);
  private final String host;
  @VisibleForTesting final int port;
  private final String schema;
  private final String connectionURL;
  private final IDatabricksComputeResource computeResource;
  private DatabricksClientType clientType;
  @VisibleForTesting final ImmutableMap<String, String> parameters;
  @VisibleForTesting final String connectionUuid;

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
    this.connectionUuid = UUID.randomUUID().toString();
    this.clientType = getClientTypeFromContext();
  }

  private DatabricksConnectionContext(
      String connectionURL, String host, ImmutableMap<String, String> parameters) {
    this.connectionURL = connectionURL;
    this.host = host;
    this.port = DEFAULT_PORT;
    this.schema = DEFAULT_SCHEMA;
    this.parameters = parameters;
    this.computeResource = null;
    this.connectionUuid = UUID.randomUUID().toString();
  }

  /**
   * Builds a map of properties from the given connection parameter string and properties object.
   *
   * @param connectionParamString the connection parameter string
   * @param properties the properties object
   * @return an immutable map of properties
   */
  public static ImmutableMap<String, String> buildPropertiesMap(
      String connectionParamString, Properties properties) {
    ImmutableMap.Builder<String, String> parametersBuilder = ImmutableMap.builder();
    String[] urlParts = connectionParamString.split(DatabricksJdbcConstants.URL_DELIMITER);
    for (String urlPart : urlParts) {
      String[] pair = urlPart.split(DatabricksJdbcConstants.PAIR_DELIMITER);
      if (pair.length == 1) {
        pair = new String[] {pair[0], ""};
      }
      parametersBuilder.put(pair[0].toLowerCase(), pair[1]);
    }
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      parametersBuilder.put(entry.getKey().toString().toLowerCase(), entry.getValue().toString());
    }
    return parametersBuilder.build();
  }

  static IDatabricksConnectionContext parseWithoutError(String url, Properties properties) {
    Matcher urlMatcher = JDBC_URL_PATTERN.matcher(url);
    if (urlMatcher.find()) {
      String host = urlMatcher.group(1).split(DatabricksJdbcConstants.PORT_DELIMITER)[0];
      // Explicitly check for null before accessing. covers cases like "jdbc:databricks://test"
      // (no <;> after host)
      String connectionParamString = urlMatcher.group(3) != null ? urlMatcher.group(3) : "";
      ImmutableMap<String, String> connectionPropertiesMap =
          buildPropertiesMap(connectionParamString, properties);
      return new DatabricksConnectionContext(url, host, connectionPropertiesMap);
    }
    return null;
  }

  /**
   * Parses connection Url and properties into a Databricks specific connection context
   *
   * @param url Databricks server connection Url
   * @param properties connection properties
   * @return a connection context
   */
  public static IDatabricksConnectionContext parse(String url, Properties properties)
      throws DatabricksSQLException {
    if (!ValidationUtil.isValidJdbcUrl(url)) {
      throw new DatabricksParsingException(
          "Invalid url " + url, DatabricksDriverErrorCode.CONNECTION_ERROR);
    }
    Matcher urlMatcher = JDBC_URL_PATTERN.matcher(url);
    if (urlMatcher.find()) {
      String hostUrlVal = urlMatcher.group(1);
      String schema = urlMatcher.group(2);
      String urlMinusHost = urlMatcher.group(3);
      String[] hostAndPort = hostUrlVal.split(DatabricksJdbcConstants.PORT_DELIMITER);
      String hostValue = hostAndPort[0];
      int portValue =
          hostAndPort.length == 2
              ? Integer.parseInt(hostAndPort[1])
              : DatabricksJdbcConstants.DEFAULT_PORT;

      ImmutableMap<String, String> propertiesMap = buildPropertiesMap(urlMinusHost, properties);
      if (propertiesMap.containsKey(PORT)) {
        try {
          portValue = Integer.parseInt(propertiesMap.get(PORT));
        } catch (NumberFormatException e) {
          throw new DatabricksParsingException(
              "Invalid port number " + propertiesMap.get(PORT),
              DatabricksDriverErrorCode.CONNECTION_ERROR);
        }
      }
      return new DatabricksConnectionContext(url, hostValue, portValue, schema, propertiesMap);
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

  @Override
  public boolean isPropertyPresent(DatabricksJdbcUrlParams urlParam) {
    return parameters.containsKey(urlParam.getParamName().toLowerCase());
  }

  @Override
  public String getHostUrl() throws DatabricksParsingException {
    LOGGER.debug("public String getHostUrl()");
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
      LOGGER.debug("URI Building failed with exception: " + e.getMessage());
      throw new DatabricksParsingException(
          "URI Building failed with exception: " + e.getMessage(),
          DatabricksDriverErrorCode.CONNECTION_ERROR);
    }
  }

  @Override
  public IDatabricksComputeResource getComputeResource() {
    return computeResource;
  }

  public String getHttpPath() {
    LOGGER.debug("String getHttpPath()");
    return getParameter(DatabricksJdbcUrlParams.HTTP_PATH);
  }

  @Override
  public String getHostForOAuth() {
    return this.host;
  }

  @Override
  public String getToken() {
    return getParameter(
        DatabricksJdbcUrlParams.PWD, getParameter(DatabricksJdbcUrlParams.PASSWORD));
  }

  @Override
  public String getPassThroughAccessToken() {
    return getParameter(DatabricksJdbcUrlParams.AUTH_ACCESS_TOKEN);
  }

  @Override
  public int getAsyncExecPollInterval() {
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.POLL_INTERVAL));
  }

  @Override
  public Boolean getDirectResultMode() {
    return Objects.equals(getParameter(DatabricksJdbcUrlParams.DIRECT_RESULT), "1");
  }

  public Cloud getCloud() throws DatabricksParsingException {
    String hostURL = getHostUrl();
    String hostName = URI.create(hostURL).getHost();
    return DatabricksEnvironment.getEnvironmentFromHostname(hostName).getCloud();
  }

  public String getGcpAuthType() throws DatabricksParsingException {
    if (parameters.containsKey(
        DatabricksJdbcUrlParams.GOOGLE_SERVICE_ACCOUNT.getParamName().toLowerCase())) {
      return DatabricksJdbcConstants.GCP_GOOGLE_ID_AUTH_TYPE;
    }
    if (parameters.containsKey(
        DatabricksJdbcUrlParams.GOOGLE_CREDENTIALS_FILE.getParamName().toLowerCase())) {
      return DatabricksJdbcConstants.GCP_GOOGLE_CREDENTIALS_AUTH_TYPE;
    }
    return DatabricksJdbcConstants.M2M_AUTH_TYPE;
  }

  @Override
  public String getClientId() throws DatabricksParsingException {
    String clientId = getParameter(DatabricksJdbcUrlParams.CLIENT_ID);
    if (nullOrEmptyString(clientId)) {
      Cloud cloud = getCloud();
      if (cloud == Cloud.AWS) {
        return DatabricksJdbcConstants.AWS_CLIENT_ID;
      } else if (cloud == Cloud.GCP) {
        return DatabricksJdbcConstants.GCP_CLIENT_ID;
      } else if (cloud == Cloud.AZURE) {
        return DatabricksJdbcConstants.AAD_CLIENT_ID;
      }
    }
    return clientId;
  }

  @Override
  public List<String> getOAuthScopesForU2M() throws DatabricksParsingException {
    if (getCloud() == Cloud.AWS || getCloud() == Cloud.GCP) {
      return Arrays.asList(
          DatabricksJdbcConstants.SQL_SCOPE, DatabricksJdbcConstants.OFFLINE_ACCESS_SCOPE);
    } else {
      // Default scope is already being set for Azure in databricks-sdk.
      return null;
    }
  }

  @Override
  public String getClientSecret() {
    return getParameter(DatabricksJdbcUrlParams.CLIENT_SECRET);
  }

  @Override
  public String getGoogleServiceAccount() {
    return getParameter(DatabricksJdbcUrlParams.GOOGLE_SERVICE_ACCOUNT);
  }

  @Override
  public String getGoogleCredentials() {
    return getParameter(DatabricksJdbcUrlParams.GOOGLE_CREDENTIALS_FILE);
  }

  @Override
  public AuthFlow getAuthFlow() {
    String authFlow = getParameter(DatabricksJdbcUrlParams.AUTH_FLOW);
    if (nullOrEmptyString(authFlow)) return AuthFlow.TOKEN_PASSTHROUGH;
    return AuthFlow.values()[Integer.parseInt(authFlow)];
  }

  @Override
  public AuthMech getAuthMech() {
    String authMech = getParameter(DatabricksJdbcUrlParams.AUTH_MECH);
    return AuthMech.parseAuthMech(authMech);
  }

  @Override
  public LogLevel getLogLevel() {
    String logLevel = getParameter(DatabricksJdbcUrlParams.LOG_LEVEL);
    if (nullOrEmptyString(logLevel)) {
      LOGGER.debug("Using default log level " + DEFAULT_LOG_LEVEL + " as none was provided.");
      return DEFAULT_LOG_LEVEL;
    }
    try {
      return getLogLevel(Integer.parseInt(logLevel));
    } catch (NumberFormatException e) {
      LOGGER.debug("Input log level is not an integer, parsing string.");
      logLevel = logLevel.toUpperCase();
    }

    try {
      return LogLevel.valueOf(logLevel);
    } catch (Exception e) {
      LOGGER.debug(
          "Using default log level " + DEFAULT_LOG_LEVEL + " as invalid level was provided.");
      return DEFAULT_LOG_LEVEL;
    }
  }

  @Override
  public String getLogPathString() {
    String parameter = getParameter(DatabricksJdbcUrlParams.LOG_PATH);
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
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.LOG_FILE_SIZE));
  }

  @Override
  public int getLogFileCount() {
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.LOG_FILE_COUNT));
  }

  @Override
  public String getClientUserAgent() {
    return getClientType().equals(DatabricksClientType.SEA)
        ? USER_AGENT_SEA_CLIENT
        : USER_AGENT_THRIFT_CLIENT;
  }

  public String getCustomerUserAgent() {
    return getParameter(DatabricksJdbcUrlParams.USER_AGENT_ENTRY);
  }

  @Override
  public CompressionCodec getCompressionCodec() {
    String compressionType =
        getParameter(
            DatabricksJdbcUrlParams.LZ4_COMPRESSION_FLAG,
            getParameter(DatabricksJdbcUrlParams.COMPRESSION_FLAG));
    return CompressionCodec.parseCompressionType(compressionType);
  }

  public DatabricksClientType getClientTypeFromContext() {
    if (computeResource instanceof AllPurposeCluster) {
      return DatabricksClientType.THRIFT;
    }
    String useThriftClient = getParameter(DatabricksJdbcUrlParams.USE_THRIFT_CLIENT);
    if (useThriftClient != null && useThriftClient.equals("1")) {
      return DatabricksClientType.THRIFT;
    }
    return DatabricksClientType.SEA;
  }

  @Override
  public DatabricksClientType getClientType() {
    return clientType;
  }

  public void setClientType(DatabricksClientType clientType) {
    this.clientType = clientType;
  }

  @Override
  public int getCloudFetchThreadPoolSize() {
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.CLOUD_FETCH_THREAD_POOL_SIZE));
  }

  @Override
  public String getCatalog() {
    return getParameter(DatabricksJdbcUrlParams.CONN_CATALOG);
  }

  @Override
  public String getSchema() {
    return getParameter(DatabricksJdbcUrlParams.CONN_SCHEMA, schema);
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
  public Map<String, String> getClientInfoProperties() {
    return ALLOWED_CLIENT_INFO_PROPERTIES.stream()
        .map(String::toLowerCase)
        .filter(parameters::containsKey)
        .collect(
            Collectors.toMap(
                key -> key,
                key ->
                    isAccessToken(key)
                        ? REDACTED_TOKEN
                        : parameters.get(key))); // mask access token
  }

  private boolean isAccessToken(String key) {
    return key.equalsIgnoreCase(DatabricksJdbcUrlParams.AUTH_ACCESS_TOKEN.getParamName());
  }

  @Override
  public boolean isAllPurposeCluster() {
    return this.computeResource instanceof AllPurposeCluster;
  }

  @Override
  public String getProxyHost() {
    return getParameter(DatabricksJdbcUrlParams.PROXY_HOST);
  }

  @Override
  public int getProxyPort() {
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.PROXY_PORT));
  }

  @Override
  public String getProxyUser() {
    return getParameter(DatabricksJdbcUrlParams.PROXY_USER);
  }

  @Override
  public String getProxyPassword() {
    return getParameter(DatabricksJdbcUrlParams.PROXY_PWD);
  }

  @Override
  public Boolean getUseProxy() {
    return Objects.equals(getParameter(DatabricksJdbcUrlParams.USE_PROXY), "1");
  }

  @Override
  public ProxyConfig.ProxyAuthType getProxyAuthType() {
    int proxyAuthTypeOrdinal =
        Integer.parseInt(getParameter(DatabricksJdbcUrlParams.PROXY_AUTH, "0"));
    return ProxyConfig.ProxyAuthType.values()[proxyAuthTypeOrdinal];
  }

  @Override
  public Boolean getUseSystemProxy() {
    return Objects.equals(getParameter(DatabricksJdbcUrlParams.USE_SYSTEM_PROXY), "1");
  }

  @Override
  public Boolean getUseCloudFetchProxy() {
    return Objects.equals(getParameter(DatabricksJdbcUrlParams.USE_CF_PROXY), "1");
  }

  @Override
  public String getCloudFetchProxyHost() {
    return getParameter(DatabricksJdbcUrlParams.CF_PROXY_HOST);
  }

  @Override
  public int getCloudFetchProxyPort() {
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.CF_PROXY_PORT));
  }

  @Override
  public String getCloudFetchProxyUser() {
    return getParameter(DatabricksJdbcUrlParams.CF_PROXY_USER);
  }

  @Override
  public String getCloudFetchProxyPassword() {
    return getParameter(DatabricksJdbcUrlParams.CF_PROXY_PWD);
  }

  @Override
  public ProxyConfig.ProxyAuthType getCloudFetchProxyAuthType() {
    int proxyAuthTypeOrdinal =
        Integer.parseInt(getParameter(DatabricksJdbcUrlParams.CF_PROXY_AUTH));
    return ProxyConfig.ProxyAuthType.values()[proxyAuthTypeOrdinal];
  }

  @Override
  public Boolean shouldEnableArrow() {
    return Objects.equals(getParameter(DatabricksJdbcUrlParams.ENABLE_ARROW), "1");
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
        LOGGER.info(
            "Using default log level " + DEFAULT_LOG_LEVEL + " as invalid level was provided.");
        return DEFAULT_LOG_LEVEL;
    }
  }

  @Override
  public Boolean shouldRetryTemporarilyUnavailableError() {
    return Objects.equals(getParameter(DatabricksJdbcUrlParams.TEMPORARILY_UNAVAILABLE_RETRY), "1");
  }

  @Override
  public Boolean shouldRetryRateLimitError() {
    return Objects.equals(getParameter(DatabricksJdbcUrlParams.RATE_LIMIT_RETRY), "1");
  }

  @Override
  public int getTemporarilyUnavailableRetryTimeout() {
    return Integer.parseInt(
        getParameter(DatabricksJdbcUrlParams.TEMPORARILY_UNAVAILABLE_RETRY_TIMEOUT));
  }

  @Override
  public int getRateLimitRetryTimeout() {
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.RATE_LIMIT_RETRY_TIMEOUT));
  }

  @Override
  public int getIdleHttpConnectionExpiry() {
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.IDLE_HTTP_CONNECTION_EXPIRY));
  }

  @Override
  public boolean supportManyParameters() {
    return getParameter(DatabricksJdbcUrlParams.SUPPORT_MANY_PARAMETERS).equals("1");
  }

  @Override
  public String getConnectionURL() {
    return connectionURL;
  }

  @Override
  public boolean checkCertificateRevocation() {
    return Objects.equals(getParameter(DatabricksJdbcUrlParams.CHECK_CERTIFICATE_REVOCATION), "1");
  }

  @Override
  public boolean acceptUndeterminedCertificateRevocation() {
    return Objects.equals(
        getParameter(DatabricksJdbcUrlParams.ACCEPT_UNDETERMINED_CERTIFICATE_REVOCATION), "1");
  }

  @Override
  public String getJWTKeyFile() {
    return getParameter(DatabricksJdbcUrlParams.JWT_KEY_FILE);
  }

  @Override
  public String getKID() {
    return getParameter(DatabricksJdbcUrlParams.JWT_KID);
  }

  @Override
  public String getJWTPassphrase() {
    return getParameter(DatabricksJdbcUrlParams.JWT_PASS_PHRASE);
  }

  @Override
  public String getJWTAlgorithm() {
    return getParameter(DatabricksJdbcUrlParams.JWT_ALGORITHM);
  }

  @Override
  public boolean useJWTAssertion() {
    return getParameter(DatabricksJdbcUrlParams.USE_JWT_ASSERTION).equals("1");
  }

  @Override
  public String getTokenEndpoint() {
    return getParameter(
        DatabricksJdbcUrlParams.OAUTH_TOKEN_ENDPOINT,
        getParameter(DatabricksJdbcUrlParams.TOKEN_ENDPOINT));
  }

  @Override
  public String getAuthEndpoint() {
    return getParameter(
        DatabricksJdbcUrlParams.OAUTH_ENDPOINT,
        getParameter(DatabricksJdbcUrlParams.AUTH_ENDPOINT));
  }

  @Override
  public boolean isOAuthDiscoveryModeEnabled() {
    // By default, set to true
    return getParameter(
            DatabricksJdbcUrlParams.OIDC_DISCOVERY_MODE,
            getParameter(DatabricksJdbcUrlParams.DISCOVERY_MODE))
        .equals("1");
  }

  @Override
  public String getOAuthDiscoveryURL() {
    return getParameter(
        DatabricksJdbcUrlParams.OIDC_DISCOVERY_ENDPOINT,
        getParameter(DatabricksJdbcUrlParams.DISCOVERY_URL));
  }

  @Override
  public String getAuthScope() {
    return getParameter(DatabricksJdbcUrlParams.AUTH_SCOPE);
  }

  @Override
  public String getOAuthRefreshToken() {
    return getParameter(
        DatabricksJdbcUrlParams.OAUTH_REFRESH_TOKEN,
        getParameter(DatabricksJdbcUrlParams.OAUTH_REFRESH_TOKEN_2));
  }

  @Override
  public Boolean getUseEmptyMetadata() {
    String param = getParameter(DatabricksJdbcUrlParams.USE_EMPTY_METADATA);
    return param != null && param.equals("1");
  }

  public String getNonProxyHosts() {
    return getParameter(DatabricksJdbcUrlParams.NON_PROXY_HOSTS);
  }

  @Override
  public String getSSLTrustStore() {
    return getParameter(DatabricksJdbcUrlParams.SSL_TRUST_STORE);
  }

  @Override
  public String getSSLTrustStorePassword() {
    return getParameter(DatabricksJdbcUrlParams.SSL_TRUST_STORE_PASSWORD);
  }

  @Override
  public String getSSLTrustStoreType() {
    return getParameter(DatabricksJdbcUrlParams.SSL_TRUST_STORE_TYPE);
  }

  @Override
  public int getMaxBatchSize() {
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.MAX_BATCH_SIZE));
  }

  @Override
  public String getConnectionUuid() {
    return connectionUuid;
  }

  @Override
  public int getTelemetryBatchSize() {
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.TELEMETRY_BATCH_SIZE));
  }

  @Override
  public boolean isTelemetryEnabled() {
    return getParameter(DatabricksJdbcUrlParams.ENABLE_TELEMETRY, "0").equals("1");
  }

  @Override
  public String getVolumeOperationAllowedPaths() {
    return getParameter(
        DatabricksJdbcUrlParams.ALLOWED_VOLUME_INGESTION_PATHS,
        getParameter(DatabricksJdbcUrlParams.ALLOWED_STAGING_INGESTION_PATHS, ""));
  }

  @Override
  public boolean isSqlExecHybridResultsEnabled() {
    return getParameter(DatabricksJdbcUrlParams.ENABLE_SQL_EXEC_HYBRID_RESULTS).equals("1");
  }

  @Override
  public String getAzureTenantId() {
    return getParameter(DatabricksJdbcUrlParams.AZURE_TENANT_ID);
  }

  @Override
  public boolean isComplexDatatypeSupportEnabled() {
    return getParameter(DatabricksJdbcUrlParams.ENABLE_COMPLEX_DATATYPE_SUPPORT).equals("1");
  }

  @Override
  public boolean isRequestTracingEnabled() {
    return getParameter(DatabricksJdbcUrlParams.ENABLE_REQUEST_TRACING).equals("1");
  }

  @Override
  public int getHttpConnectionPoolSize() {
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.HTTP_CONNECTION_POOL_SIZE));
  }

  private static boolean nullOrEmptyString(String s) {
    return s == null || s.isEmpty();
  }

  private String getSSLMode() {
    return getParameter(DatabricksJdbcUrlParams.SSL);
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
    throw new DatabricksParsingException(
        "Invalid HTTP Path provided " + this.getHttpPath(),
        DatabricksDriverErrorCode.CONNECTION_ERROR);
  }

  private String getParameter(DatabricksJdbcUrlParams key) {
    return this.parameters.getOrDefault(key.getParamName().toLowerCase(), key.getDefaultValue());
  }

  private String getParameter(DatabricksJdbcUrlParams key, String defaultValue) {
    return this.parameters.getOrDefault(key.getParamName().toLowerCase(), defaultValue);
  }
}
