package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;
import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.AUTH_SCOPE;
import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.DEFAULT_STRING_COLUMN_LENGTH;
import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_ROW_LIMIT_PER_BLOCK;
import static com.databricks.jdbc.common.util.UserAgentManager.USER_AGENT_SEA_CLIENT;
import static com.databricks.jdbc.common.util.UserAgentManager.USER_AGENT_THRIFT_CLIENT;
import static com.databricks.jdbc.common.util.WildcardUtil.isNullOrEmpty;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.*;
import com.databricks.jdbc.common.safe.DatabricksDriverFeatureFlagsContextFactory;
import com.databricks.jdbc.common.util.ValidationUtil;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.DatabricksEnvironment;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.utils.Cloud;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import org.apache.http.client.utils.URIBuilder;

public class DatabricksConnectionContext implements IDatabricksConnectionContext {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksConnectionContext.class);

  private static final String SQL_EXEC_FLAG_NAME =
      "databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc";

  private final String host;
  @VisibleForTesting final int port;
  private final String schema;
  private final String connectionURL;
  private final IDatabricksComputeResource computeResource;
  private final Map<String, String> customHeaders;
  private Supplier<DatabricksClientType> clientTypeSupplier;
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
    this.customHeaders = parseCustomHeaders(parameters);
    this.computeResource = buildCompute();
    this.connectionUuid = UUID.randomUUID().toString();
    this.clientTypeSupplier =
        new Supplier<>() {
          private DatabricksClientType cType;

          @Override
          public synchronized DatabricksClientType get() {
            if (cType == null) {
              cType = getClientTypeFromContext();
            }
            return cType;
          }
        };
  }

  private DatabricksConnectionContext(
      String connectionURL, String host, ImmutableMap<String, String> parameters) {
    this.connectionURL = connectionURL;
    this.host = host;
    this.port = DEFAULT_PORT;
    this.schema = DEFAULT_SCHEMA;
    this.parameters = parameters;
    this.customHeaders = parseCustomHeaders(parameters);
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
    // check if connectionParamString is empty or null
    if (!isNullOrEmpty(connectionParamString)) {
      String[] urlParts = connectionParamString.split(DatabricksJdbcConstants.URL_DELIMITER);
      for (String urlPart : urlParts) {
        String[] pair = urlPart.split(DatabricksJdbcConstants.PAIR_DELIMITER);
        if (pair.length == 1) {
          pair = new String[] {pair[0], ""};
        }
        if (pair[0].startsWith(DatabricksJdbcUrlParams.HTTP_HEADERS.getParamName())) {
          parametersBuilder.put(pair[0], pair[1]);
        } else {
          parametersBuilder.put(pair[0].toLowerCase(), pair[1]);
        }
      }
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
      String schema =
          Objects.equals(urlMatcher.group(2), EMPTY_STRING) ? null : urlMatcher.group(2);
      String urlMinusHost = urlMatcher.group(3);
      String[] hostAndPort = hostUrlVal.split(DatabricksJdbcConstants.PORT_DELIMITER);
      String hostValue = hostAndPort[0];
      int portValue =
          hostAndPort.length == 2
              ? Integer.parseInt(hostAndPort[1])
              : DatabricksJdbcConstants.DEFAULT_PORT;

      ImmutableMap<String, String> propertiesMap = buildPropertiesMap(urlMinusHost, properties);

      // Validate all input properties
      ValidationUtil.validateInputProperties(propertiesMap);

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
      throw new DatabricksValidationException("Connection Context invalid state error");
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
  public String getHost() {
    return this.host;
  }

  @Override
  public IDatabricksComputeResource getComputeResource() {
    return computeResource;
  }

  public String getHttpPath() {
    LOGGER.debug("String getHttpPath()");
    return getParameter(DatabricksJdbcUrlParams.HTTP_PATH);
  }

  public boolean getEnableSQLValidationForIsValid() {
    LOGGER.debug("String getEnableSQLValidationForIsValid()");
    return getParameter(DatabricksJdbcUrlParams.ENABLE_SQL_VALIDATION_FOR_IS_VALID, "0")
        .equals("1");
  }

  public boolean getEnableMultipleCatalogSupport() {
    return getParameter(DatabricksJdbcUrlParams.ENABLE_MULTIPLE_CATALOG_SUPPORT, "1").equals("1");
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
    String clientId = getNullableClientId();
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
  public String getNullableClientId() {
    return getParameter(DatabricksJdbcUrlParams.CLIENT_ID);
  }

  @Override
  public List<String> getOAuthScopesForU2M() throws DatabricksParsingException {
    if (getParameter(AUTH_SCOPE) != null) {
      return Collections.singletonList(getAuthScope());
    }
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

  @Override
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
    // First we will check if user as explicitly provided a client type, and we will honour that
    String useThriftClient = getParameterIgnoreDefault(DatabricksJdbcUrlParams.USE_THRIFT_CLIENT);
    if (useThriftClient != null) {
      if (useThriftClient.equals("1")) {
        return DatabricksClientType.THRIFT;
      } else if (useThriftClient.equals("0")) {
        return DatabricksClientType.SEA;
      }
    }
    // Now, user has not provided a value, we will decide based on our checks
    // Check if Arrow is disabled - Thrift is required for inline mode
    if (!Objects.equals(getParameter(DatabricksJdbcUrlParams.ENABLE_ARROW), "1")) {
      return DatabricksClientType.THRIFT;
    }
    // Check if CloudFetch is disabled - Thrift is required for inline mode
    if (!Objects.equals(getParameter(DatabricksJdbcUrlParams.ENABLE_CLOUD_FETCH), "1")) {
      return DatabricksClientType.THRIFT;
    }
    // Check feature flag to determine if SEA client should be enabled
    if (DatabricksDriverFeatureFlagsContextFactory.getInstance(this)
        .isFeatureEnabled(SQL_EXEC_FLAG_NAME)) {
      return DatabricksClientType.SEA;
    }
    // Default to THRIFT if feature flag is not enabled or cannot be determined
    return DatabricksClientType.THRIFT;
  }

  @Override
  public DatabricksClientType getClientType() {
    return clientTypeSupplier.get();
  }

  public void setClientType(DatabricksClientType clientType) {
    this.clientTypeSupplier = () -> clientType;
  }

  @Override
  public int getCloudFetchThreadPoolSize() {
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.CLOUD_FETCH_THREAD_POOL_SIZE));
  }

  @Override
  public double getCloudFetchSpeedThreshold() {
    return Double.parseDouble(getParameter(DatabricksJdbcUrlParams.CLOUD_FETCH_SPEED_THRESHOLD));
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
    Map<String, String> sessionConfigs =
        this.parameters.entrySet().stream()
            .filter(
                e ->
                    ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.keySet().stream()
                        .anyMatch(allowedConf -> allowedConf.toLowerCase().equals(e.getKey())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // Add metric view metadata configuration if enabled
    if (getEnableMetricViewMetadata()) {
      sessionConfigs.put("spark.sql.thriftserver.metadata.metricview.enabled", "true");
    }

    return sessionConfigs;
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

  public Map<String, String> getCustomHeaders() {
    return this.customHeaders;
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
  public String getIdentityFederationClientId() {
    return getParameter(DatabricksJdbcUrlParams.IDENTITY_FEDERATION_CLIENT_ID);
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
  public List<Integer> getOAuth2RedirectUrlPorts() {
    String portsStr = getParameter(DatabricksJdbcUrlParams.OAUTH_REDIRECT_URL_PORT);

    try {
      // Parse comma-separated list of ports
      return Arrays.stream(portsStr.split(","))
          .map(String::trim)
          .filter(s -> !s.isEmpty())
          .map(Integer::parseInt)
          .collect(Collectors.toList());
    } catch (NumberFormatException e) {
      String errorMessage =
          String.format("Invalid port format in OAuth2RedirectUrlPort: %s.", portsStr);
      LOGGER.error(e, errorMessage);
      throw new DatabricksDriverException(
          errorMessage, DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }
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
  public String getSSLTrustStoreProvider() {
    return getParameter(DatabricksJdbcUrlParams.SSL_TRUST_STORE_PROVIDER);
  }

  @Override
  public String getSSLKeyStore() {
    return getParameter(DatabricksJdbcUrlParams.SSL_KEY_STORE);
  }

  @Override
  public String getSSLKeyStorePassword() {
    return getParameter(DatabricksJdbcUrlParams.SSL_KEY_STORE_PASSWORD);
  }

  @Override
  public String getSSLKeyStoreType() {
    return getParameter(DatabricksJdbcUrlParams.SSL_KEY_STORE_TYPE);
  }

  @Override
  public String getSSLKeyStoreProvider() {
    return getParameter(DatabricksJdbcUrlParams.SSL_KEY_STORE_PROVIDER);
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
  public int getBatchInsertSize() {
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.BATCH_INSERT_SIZE));
  }

  @Override
  public boolean isTelemetryEnabled() {
    return getParameter(DatabricksJdbcUrlParams.ENABLE_TELEMETRY).equals("1");
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
  public boolean isSqlExecDirectResultsEnabled() {
    return getParameter(DatabricksJdbcUrlParams.ENABLE_SQL_EXEC_DIRECT_RESULTS).equals("1");
  }

  @Override
  public String getAzureTenantId() {
    return getParameter(DatabricksJdbcUrlParams.AZURE_TENANT_ID);
  }

  @Override
  public int getDefaultStringColumnLength() {
    try {
      int defaultStringColumnLength = Integer.parseInt(getParameter(DEFAULT_STRING_COLUMN_LENGTH));
      if (defaultStringColumnLength < 0
          || defaultStringColumnLength > MAX_DEFAULT_STRING_COLUMN_LENGTH) {
        LOGGER.warn(
            "DefaultStringColumnLength value {} is out of bounds (0 to 32767). Falling back to default value 255.",
            defaultStringColumnLength);
        return DEFUALT_STRING_COLUMN_LENGTH;
      }
      return defaultStringColumnLength;
    } catch (NumberFormatException e) {
      LOGGER.warn(
          "Invalid number format for DefaultStringColumnLength. Falling back to default value 255.");
      return DEFUALT_STRING_COLUMN_LENGTH;
    }
  }

  @Override
  public int getMaxDBFSConcurrentPresignedRequests() {
    try {
      return Integer.parseInt(
          getParameter(DatabricksJdbcUrlParams.MAX_CONCURRENT_PRESIGNED_REQUESTS));
    } catch (NumberFormatException e) {
      LOGGER.warn(
          "Invalid number format for MaxVolumeOperationConcurrentPresignedRequests. Falling back to default value 50.");
      return DEFAULT_MAX_CONCURRENT_PRESIGNED_REQUESTS;
    }
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

  @Override
  public boolean allowSelfSignedCerts() {
    return getParameter(DatabricksJdbcUrlParams.ALLOW_SELF_SIGNED_CERTS).equals("1");
  }

  @Override
  public boolean useSystemTrustStore() {
    return getParameter(DatabricksJdbcUrlParams.USE_SYSTEM_TRUST_STORE).equals("1");
  }

  @Override
  public List<Integer> getUCIngestionRetriableHttpCodes() {
    return Arrays.stream(
            getParameter(
                    DatabricksJdbcUrlParams.VOLUME_OPERATION_RETRYABLE_HTTP_CODE,
                    getParameter(DatabricksJdbcUrlParams.UC_INGESTION_RETRIABLE_HTTP_CODE))
                .split(","))
        .map(String::trim)
        .filter(num -> num.matches("\\d+")) // Ensure only positive integers
        .map(Integer::parseInt)
        .collect(Collectors.toList());
  }

  @Override
  public int getUCIngestionRetryTimeoutSeconds() {
    // The Url param takes value in minutes
    return 60
        * Integer.parseInt(
            getParameter(
                DatabricksJdbcUrlParams.VOLUME_OPERATION_RETRY_TIMEOUT,
                getParameter(DatabricksJdbcUrlParams.UC_INGESTION_RETRY_TIMEOUT)));
  }

  @Override
  public String getAzureWorkspaceResourceId() {
    return getParameter(DatabricksJdbcUrlParams.AZURE_WORKSPACE_RESOURCE_ID);
  }

  @Override
  public int getRowsFetchedPerBlock() {
    int maxRows = DEFAULT_ROW_LIMIT_PER_BLOCK;
    try {
      maxRows = Integer.parseInt(getParameter(DatabricksJdbcUrlParams.ROWS_FETCHED_PER_BLOCK));
    } catch (NumberFormatException e) {
      LOGGER.warn("Invalid value for RowsFetchedPerBlock, using default value");
    }
    return maxRows;
  }

  /** {@inheritDoc} */
  @Override
  public int getSocketTimeout() {
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.SOCKET_TIMEOUT));
  }

  @Override
  public String getTokenCachePassPhrase() {
    return getParameter(DatabricksJdbcUrlParams.TOKEN_CACHE_PASS_PHRASE);
  }

  @Override
  public boolean isTokenCacheEnabled() {
    return getParameter(DatabricksJdbcUrlParams.ENABLE_TOKEN_CACHE).equals("1");
  }

  @Override
  public String getApplicationName() {
    return getParameter(DatabricksJdbcUrlParams.APPLICATION_NAME);
  }

  /** {@inheritDoc} */
  @Override
  public int getChunkReadyTimeoutSeconds() {
    return Integer.parseInt(getParameter(DatabricksJdbcUrlParams.CHUNK_READY_TIMEOUT_SECONDS));
  }

  @Override
  public boolean isTelemetryCircuitBreakerEnabled() {
    return getParameter(DatabricksJdbcUrlParams.TELEMETRY_CIRCUIT_BREAKER_ENABLED).equals("1");
  }

  @Override
  public int getHttpMaxConnectionsPerRoute() {
    int maxConnectionsPerRoute = DEFAULT_MAX_HTTP_CONNECTIONS_PER_ROUTE;
    try {
      maxConnectionsPerRoute =
          Integer.parseInt(getParameter(DatabricksJdbcUrlParams.HTTP_MAX_CONNECTIONS_PER_ROUTE));
    } catch (NumberFormatException e) {
      LOGGER.warn("Invalid value for HttpMaxConnectionsPerRoutes");
    }
    return maxConnectionsPerRoute;
  }

  @Override
  public Integer getHttpConnectionRequestTimeout() {
    String httpConnectionRequestTimeout =
        getParameter(DatabricksJdbcUrlParams.HTTP_CONNECTION_REQUEST_TIMEOUT);
    if (!Strings.isNullOrEmpty(httpConnectionRequestTimeout)) {
      try {
        return Integer.parseInt(httpConnectionRequestTimeout);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid value for HttpConnectionRequestTimeout");
      }
    }
    return null;
  }

  @Override
  public boolean enableShowCommandsForGetFunctions() {
    return getParameter(DatabricksJdbcUrlParams.ENABLE_SHOW_COMMAND_FOR_GET_FUNCTIONS).equals("1");
  }

  @Override
  public boolean getEnableMetricViewMetadata() {
    return getParameter(DatabricksJdbcUrlParams.ENABLE_METRIC_VIEW_METADATA).equals("1");
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

  private String getParameterIgnoreDefault(DatabricksJdbcUrlParams key) {
    return this.parameters.getOrDefault(key.getParamName().toLowerCase(), null);
  }

  private String getParameter(DatabricksJdbcUrlParams key, String defaultValue) {
    return this.parameters.getOrDefault(key.getParamName().toLowerCase(), defaultValue);
  }

  private Map<String, String> parseCustomHeaders(ImmutableMap<String, String> parameters) {
    String filterPrefix = DatabricksJdbcUrlParams.HTTP_HEADERS.getParamName();

    return parameters.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(filterPrefix))
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().substring(filterPrefix.length()), Map.Entry::getValue));
  }

  @Override
  public boolean forceEnableTelemetry() {
    return getParameter(DatabricksJdbcUrlParams.FORCE_ENABLE_TELEMETRY).equals("1");
  }

  @Override
  public int getTelemetryFlushIntervalInMilliseconds() {
    // There is a minimum threshold of 1000ms for the flush interval
    return Math.max(
        1000, Integer.parseInt(getParameter(DatabricksJdbcUrlParams.TELEMETRY_FLUSH_INTERVAL)));
  }

  @Override
  public boolean isBatchedInsertsEnabled() {
    return getParameter(DatabricksJdbcUrlParams.ENABLE_BATCHED_INSERTS).equals("1");
  }

  @Override
  public boolean getIgnoreTransactions() {
    return getParameter(DatabricksJdbcUrlParams.IGNORE_TRANSACTIONS, "0").equals("1");
  }

  @Override
  public boolean getFetchAutoCommitFromServer() {
    return getParameter(DatabricksJdbcUrlParams.FETCH_AUTOCOMMIT_FROM_SERVER).equals("1");
  }

  @Override
  public TelemetryLogLevel getTelemetryLogLevel() {
    return TelemetryLogLevel.parse(
        getParameter(DatabricksJdbcUrlParams.TELEMETRY_LOG_LEVEL), TelemetryLogLevel.DEBUG);
  }

  @Override
  public boolean isSeaSyncMetadataEnabled() {
    return getParameter(DatabricksJdbcUrlParams.ENABLE_SEA_SYNC_METADATA).equals("1");
  }

  @Override
  public boolean getDisableOauthRefreshToken() {
    return getParameter(DatabricksJdbcUrlParams.DISABLE_OAUTH_REFRESH_TOKEN, "1").equals("1");
  }
}
