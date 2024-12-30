package com.databricks.jdbc.model.telemetry;

import com.databricks.jdbc.common.AuthFlow;
import com.databricks.jdbc.common.AuthMech;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.model.telemetry.enums.DriverProxy;
import com.databricks.sdk.core.ProxyConfig;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DriverConnectionParameters {
  @JsonProperty("http_path")
  String httpPath;

  @JsonProperty("mode")
  DatabricksClientType driverMode;

  @JsonProperty("host_url")
  String hostUrl;

  @JsonProperty("use_proxy")
  boolean useProxy;

  @JsonProperty("auth_mech")
  AuthMech authMech;

  @JsonProperty("proxy_host")
  String proxyHost;

  @JsonProperty("proxy_port")
  int proxyPort;

  @JsonProperty("proxy_type")
  DriverProxy proxyType;

  @JsonProperty("non_proxy_hosts")
  String non_proxy_hosts;

  @JsonProperty("use_system_proxy")
  boolean useSystemProxy;

  @JsonProperty("use_cf_proxy")
  boolean useCfProxy;

  @JsonProperty("cf_proxy_host")
  String cfProxyHost;

  @JsonProperty("cf_proxy_port")
  int cfProxyPort;

  @JsonProperty("cf_proxy_auth")
  DriverProxy cfProxyAuth;

  @JsonProperty("auth_flow")
  AuthFlow driverAuthFlow;

  @JsonProperty("discovery_mode_enabled")
  boolean discoveryModeEnabled;

  @JsonProperty("auth_scope")
  String authScope;

  @JsonProperty("discovery_url")
  String discoveryUrl;

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

  public DriverConnectionParameters setHttpPath(String httpPath) {
    this.httpPath = httpPath;
    return this;
  }

  public DriverConnectionParameters setDriverMode(DatabricksClientType clientType) {
    this.driverMode = clientType;
    return this;
  }

  public DriverConnectionParameters setHostUrl(String hostUrl) {
    this.hostUrl = hostUrl;
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

  public DriverConnectionParameters setProxyHost(String proxyHost) {
    this.proxyHost = proxyHost;
    return this;
  }

  public DriverConnectionParameters setProxyPort(int proxyPort) {
    this.proxyPort = proxyPort;
    return this;
  }

  public DriverConnectionParameters setProxyType(ProxyConfig.ProxyAuthType proxyType) {
    this.proxyType = DriverProxy.getDriverProxy(proxyType);
    return this;
  }

  public DriverConnectionParameters setNonProxyHosts(String non_proxy_hosts) {
    this.non_proxy_hosts = non_proxy_hosts;
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

  public DriverConnectionParameters setCfProxyHost(String cfProxyHost) {
    this.cfProxyHost = cfProxyHost;
    return this;
  }

  public DriverConnectionParameters setCfProxyPort(int cfProxyPort) {
    this.cfProxyPort = cfProxyPort;
    return this;
  }

  public DriverConnectionParameters setCfProxyAuth(ProxyConfig.ProxyAuthType cfProxyAuthType) {
    this.cfProxyAuth = DriverProxy.getDriverProxy(cfProxyAuthType);
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
}
