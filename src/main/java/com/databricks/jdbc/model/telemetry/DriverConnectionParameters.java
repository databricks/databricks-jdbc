package com.databricks.jdbc.model.telemetry;

import com.databricks.jdbc.common.AuthFlow;
import com.databricks.jdbc.common.AuthMech;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class DriverConnectionParameters {
  @JsonProperty("http_path")
  String httpPath;

  @JsonProperty("mode")
  DatabricksClientType driverMode;

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

  @JsonProperty("non_proxy_hosts")
  List<String> non_proxy_hosts;

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

  public DriverConnectionParameters setUseProxy(boolean useProxy) {
    this.useProxy = useProxy;
    return this;
  }

  public DriverConnectionParameters setAuthMech(AuthMech authMech) {
    this.authMech = authMech;
    return this;
  }

  public DriverConnectionParameters setNonProxyHosts(List<String> non_proxy_hosts) {
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
        .add("nonProxyHosts", non_proxy_hosts)
        .add("useSystemProxy", useSystemProxy)
        .add("useCfProxy", useCfProxy)
        .add("proxyHostDetails", proxyHostDetails)
        .add("cfProxyHostDetails", cfProxyHostDetails)
        .add("discoveryModeEnabled", discoveryModeEnabled)
        .add("discoveryUrl", discoveryUrl)
        .add("useEmptyMetadata", useEmptyMetadata)
        .add("supportManyParameters", supportManyParameters)
        .add("sslTrustStoreType", sslTrustStoreType)
        .add("checkCertificateRevocation", checkCertificateRevocation)
        .add("acceptUndeterminedCertificateRevocation", acceptUndeterminedCertificateRevocation)
        .toString();
  }
}
