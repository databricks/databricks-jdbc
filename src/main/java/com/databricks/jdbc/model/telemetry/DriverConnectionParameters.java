package com.databricks.jdbc.model.telemetry;

import com.databricks.jdbc.common.AuthFlow;
import com.databricks.jdbc.common.AuthMech;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class DriverConnectionParameters {
  @JsonProperty("http_path")
  String httpPath;

  @JsonProperty("mode")
  String driverMode;

  @JsonProperty("host_info")
  HostDetails hostDetails;

  @JsonProperty("auth_mech")
  AuthMech authMech;

  @JsonProperty("auth_flow")
  AuthFlow driverAuthFlow;

  @JsonProperty("auth_scope")
  String authScope;

  @JsonProperty("use_proxy")
  boolean useProxy;

  @JsonProperty("use_system_proxy")
  boolean useSystemProxy;

  @JsonProperty("use_cf_proxy")
  boolean useCfProxy;

  @JsonProperty("proxy_host_info")
  HostDetails proxyHostDetails;

  @JsonProperty("cf_proxy_host_info")
  HostDetails cfProxyHostDetails;

  @JsonProperty("discovery_mode_enabled")
  boolean discoveryModeEnabled;

  @JsonProperty("discovery_url")
  String discoveryUrl;

  @JsonProperty("identity_federation_client_id")
  String identityFederationClientId;

  @JsonProperty("use_empty_metadata")
  boolean useEmptyMetadata;

  @JsonProperty("support_many_parameters")
  boolean supportManyParameters;

  @JsonProperty("ssl_trust_store_type")
  String sslTrustStoreType;

  @JsonProperty("check_certificate_revocation")
  boolean checkCertificateRevocation;

  @JsonProperty("accept_undetermined_certificate_revocation")
  boolean acceptUndeterminedCertificateRevocation;

  @JsonProperty("enable_arrow")
  boolean enableArrow;

  @JsonProperty("enable_direct_results")
  boolean enableDirectResults;

  @JsonProperty("enable_jwt_assertion")
  boolean enableJwtAssertion;

  @JsonProperty("jwt_key_file")
  String jwtKeyFile;

  @JsonProperty("jwt_algorithm")
  String jwtAlgorithm;

  @JsonProperty("google_service_account")
  String googleServiceAccount;

  @JsonProperty("google_credential_file_path")
  String googleCredentialFilePath;

  @JsonProperty("allowed_volume_ingestion_paths")
  String allowedVolumeIngestionPaths;

  @JsonProperty("enable_complex_datatype_support")
  boolean enableComplexDatatypeSupport;

  @JsonProperty("enable_geospatial_support")
  boolean enableGeoSpatialSupport;

  @JsonProperty("azure_workspace_resource_id")
  String azureWorkspaceResourceId;

  @JsonProperty("azure_tenant_id")
  String azureTenantId;

  @JsonProperty("string_column_length")
  int stringColumnLength;

  @JsonProperty("socket_timeout")
  int socketTimeout;

  @JsonProperty("enable_token_cache")
  boolean enableTokenCache;

  @JsonProperty("auth_endpoint")
  String authEndpoint;

  @JsonProperty("token_endpoint")
  String tokenEndpoint;

  @JsonProperty("non_proxy_hosts")
  List<String> nonProxyHosts;

  @JsonProperty("http_connection_pool_size")
  int httpConnectionPoolSize;

  @JsonProperty("enable_sea_hybrid_results")
  boolean enableSeaHybridResults;

  @JsonProperty("allow_self_signed_support")
  boolean allowSelfSignedSupport;

  @JsonProperty("use_system_trust_store")
  boolean useSystemTrustStore;

  @JsonProperty("rows_fetched_per_block")
  int rowsFetchedPerBlock;

  @JsonProperty("async_poll_interval_millis")
  int asyncPollIntervalMillis;

  @JsonProperty("enable_metric_view_metadata")
  boolean enableMetricViewMetadata;

  @JsonProperty("query_tags")
  String queryTags;

  public DriverConnectionParameters setHttpPath(String httpPath) {
    this.httpPath = httpPath;
    return this;
  }

  public DriverConnectionParameters setDriverMode(String clientType) {
    this.driverMode = clientType.toString();
    return this;
  }

  public DriverConnectionParameters setUseProxy(boolean useProxy) {
    this.useProxy = useProxy;
    return this;
  }

  public DriverConnectionParameters setAuthMech(AuthMech authMech) {
    this.authMech = authMech;
    return this;
  }

  public DriverConnectionParameters setUseSystemProxy(boolean useSystemProxy) {
    this.useSystemProxy = useSystemProxy;
    return this;
  }

  public DriverConnectionParameters setUseCfProxy(boolean useCfProxy) {
    this.useCfProxy = useCfProxy;
    return this;
  }

  public DriverConnectionParameters setHostDetails(HostDetails hostDetails) {
    this.hostDetails = hostDetails;
    return this;
  }

  public DriverConnectionParameters setCfProxyHostDetails(HostDetails cfProxyHostDetails) {
    this.cfProxyHostDetails = cfProxyHostDetails;
    return this;
  }

  public DriverConnectionParameters setProxyHostDetails(HostDetails proxyHostDetails) {
    this.proxyHostDetails = proxyHostDetails;
    return this;
  }

  public DriverConnectionParameters setDriverAuthFlow(AuthFlow driverAuthFlow) {
    this.driverAuthFlow = driverAuthFlow;
    return this;
  }

  public DriverConnectionParameters setDiscoveryModeEnabled(boolean discoveryModeEnabled) {
    this.discoveryModeEnabled = discoveryModeEnabled;
    return this;
  }

  public DriverConnectionParameters setAuthScope(String authScope) {
    this.authScope = authScope;
    return this;
  }

  public DriverConnectionParameters setDiscoveryUrl(String discoveryUrl) {
    this.discoveryUrl = discoveryUrl;
    return this;
  }

  public DriverConnectionParameters setIdentityFederationClientId(
      String identityFederationClientId) {
    this.identityFederationClientId = identityFederationClientId;
    return this;
  }

  public DriverConnectionParameters setUseEmptyMetadata(boolean useEmptyMetadata) {
    this.useEmptyMetadata = useEmptyMetadata;
    return this;
  }

  public DriverConnectionParameters setSupportManyParameters(boolean supportManyParameters) {
    this.supportManyParameters = supportManyParameters;
    return this;
  }

  public DriverConnectionParameters setSslTrustStoreType(String sslTrustStoreType) {
    this.sslTrustStoreType = sslTrustStoreType;
    return this;
  }

  public DriverConnectionParameters setCheckCertificateRevocation(
      boolean checkCertificateRevocation) {
    this.checkCertificateRevocation = checkCertificateRevocation;
    return this;
  }

  public DriverConnectionParameters setAcceptUndeterminedCertificateRevocation(
      boolean acceptUndeterminedCertificateRevocation) {
    this.acceptUndeterminedCertificateRevocation = acceptUndeterminedCertificateRevocation;
    return this;
  }

  public DriverConnectionParameters setEnableArrow(boolean enableArrow) {
    this.enableArrow = enableArrow;
    return this;
  }

  public DriverConnectionParameters setEnableDirectResults(boolean enableDirectResults) {
    this.enableDirectResults = enableDirectResults;
    return this;
  }

  public DriverConnectionParameters setEnableJwtAssertion(boolean enableJwtAssertion) {
    this.enableJwtAssertion = enableJwtAssertion;
    return this;
  }

  public DriverConnectionParameters setJwtKeyFile(String jwtKeyFile) {
    this.jwtKeyFile = jwtKeyFile;
    return this;
  }

  public DriverConnectionParameters setJwtAlgorithm(String jwtAlgorithm) {
    this.jwtAlgorithm = jwtAlgorithm;
    return this;
  }

  public DriverConnectionParameters setGoogleServiceAccount(String googleServiceAccount) {
    this.googleServiceAccount = googleServiceAccount;
    return this;
  }

  public DriverConnectionParameters setGoogleCredentialFilePath(String googleCredentialFilePath) {
    this.googleCredentialFilePath = googleCredentialFilePath;
    return this;
  }

  public DriverConnectionParameters setAllowedVolumeIngestionPaths(
      String allowedVolumeIngestionPaths) {
    this.allowedVolumeIngestionPaths = allowedVolumeIngestionPaths;
    return this;
  }

  public DriverConnectionParameters setEnableComplexDatatypeSupport(
      boolean enableComplexDatatypeSupport) {
    this.enableComplexDatatypeSupport = enableComplexDatatypeSupport;
    return this;
  }

  public DriverConnectionParameters setEnableGeoSpatialSupport(boolean enableGeoSpatialSupport) {
    this.enableGeoSpatialSupport = enableGeoSpatialSupport;
    return this;
  }

  public DriverConnectionParameters setAzureWorkspaceResourceId(String azureWorkspaceResourceId) {
    this.azureWorkspaceResourceId = azureWorkspaceResourceId;
    return this;
  }

  public DriverConnectionParameters setAzureTenantId(String azureTenantId) {
    this.azureTenantId = azureTenantId;
    return this;
  }

  public DriverConnectionParameters setStringColumnLength(int stringColumnLength) {
    this.stringColumnLength = stringColumnLength;
    return this;
  }

  public DriverConnectionParameters setSocketTimeout(int socketTimeout) {
    this.socketTimeout = socketTimeout;
    return this;
  }

  public DriverConnectionParameters setEnableTokenCache(boolean enableTokenCache) {
    this.enableTokenCache = enableTokenCache;
    return this;
  }

  public DriverConnectionParameters setAuthEndpoint(String authEndpoint) {
    this.authEndpoint = authEndpoint;
    return this;
  }

  public DriverConnectionParameters setTokenEndpoint(String tokenEndpoint) {
    this.tokenEndpoint = tokenEndpoint;
    return this;
  }

  public DriverConnectionParameters setNonProxyHosts(List<String> nonProxyHosts) {
    this.nonProxyHosts = nonProxyHosts;
    return this;
  }

  public DriverConnectionParameters setHttpConnectionPoolSize(int httpConnectionPoolSize) {
    this.httpConnectionPoolSize = httpConnectionPoolSize;
    return this;
  }

  public DriverConnectionParameters setEnableSeaHybridResults(boolean enableSeaHybridResults) {
    this.enableSeaHybridResults = enableSeaHybridResults;
    return this;
  }

  public DriverConnectionParameters setAllowSelfSignedSupport(boolean allowSelfSignedSupport) {
    this.allowSelfSignedSupport = allowSelfSignedSupport;
    return this;
  }

  public DriverConnectionParameters setUseSystemTrustStore(boolean useSystemTrustStore) {
    this.useSystemTrustStore = useSystemTrustStore;
    return this;
  }

  public DriverConnectionParameters setRowsFetchedPerBlock(int rowsFetchedPerBlock) {
    this.rowsFetchedPerBlock = rowsFetchedPerBlock;
    return this;
  }

  public DriverConnectionParameters setAsyncPollIntervalMillis(int asyncPollIntervalMillis) {
    this.asyncPollIntervalMillis = asyncPollIntervalMillis;
    return this;
  }

  public DriverConnectionParameters setEnableMetricViewMetadata(boolean enableMetricViewMetadata) {
    this.enableMetricViewMetadata = enableMetricViewMetadata;
    return this;
  }

  public DriverConnectionParameters setQueryTags(String queryTags) {
    this.queryTags = queryTags;
    return this;
  }

  @Override
  public String toString() {
    return new ToStringer(DriverConnectionParameters.class)
        .add("httpPath", httpPath)
        .add("driverMode", driverMode)
        .add("hostDetails", hostDetails)
        .add("authMech", authMech)
        .add("driverAuthFlow", driverAuthFlow)
        .add("authScope", authScope)
        .add("useProxy", useProxy)
        .add("useSystemProxy", useSystemProxy)
        .add("useCfProxy", useCfProxy)
        .add("proxyHostDetails", proxyHostDetails)
        .add("cfProxyHostDetails", cfProxyHostDetails)
        .add("discoveryModeEnabled", discoveryModeEnabled)
        .add("discoveryUrl", discoveryUrl)
        .add("identityFederationClientId", identityFederationClientId)
        .add("useEmptyMetadata", useEmptyMetadata)
        .add("supportManyParameters", supportManyParameters)
        .add("sslTrustStoreType", sslTrustStoreType)
        .add("checkCertificateRevocation", checkCertificateRevocation)
        .add("acceptUndeterminedCertificateRevocation", acceptUndeterminedCertificateRevocation)
        .add("enableArrow", enableArrow)
        .add("enableDirectResults", enableDirectResults)
        .add("enableJwtAssertion", enableJwtAssertion)
        .add("jwtKeyFile", jwtKeyFile)
        .add("jwtAlgorithm", jwtAlgorithm)
        .add("googleServiceAccount", googleServiceAccount)
        .add("googleCredentialFilePath", googleCredentialFilePath)
        .add("allowedVolumeIngestionPaths", allowedVolumeIngestionPaths)
        .add("enableComplexDatatypeSupport", enableComplexDatatypeSupport)
        .add("enableGeoSpatialSupport", enableGeoSpatialSupport)
        .add("azureWorkspaceResourceId", azureWorkspaceResourceId)
        .add("azureTenantId", azureTenantId)
        .add("stringColumnLength", stringColumnLength)
        .add("socketTimeout", socketTimeout)
        .add("enableTokenCache", enableTokenCache)
        .add("authEndpoint", authEndpoint)
        .add("tokenEndpoint", tokenEndpoint)
        .add("nonProxyHosts", nonProxyHosts)
        .add("httpConnectionPoolSize", httpConnectionPoolSize)
        .add("enableSeaHybridResults", enableSeaHybridResults)
        .add("allowSelfSignedSupport", allowSelfSignedSupport)
        .add("useSystemTrustStore", useSystemTrustStore)
        .add("rowsFetchedPerBlock", rowsFetchedPerBlock)
        .add("asyncPollIntervalMillis", asyncPollIntervalMillis)
        .add("enableMetricViewMetadata", enableMetricViewMetadata)
        .add("queryTags", queryTags)
        .toString();
  }
}
