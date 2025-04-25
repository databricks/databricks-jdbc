package com.databricks.jdbc.model.telemetry;

import com.databricks.jdbc.model.telemetry.enums.DriverProxy;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HostDetails {
  @JsonProperty("host_url")
  String hostUrl;

  @JsonProperty("port")
  int port;

  @JsonProperty("proxy_auth_type")
  DriverProxy proxyType;

  public HostDetails setProxyType(ProxyConfig.ProxyAuthType proxyType) {
    this.proxyType = DriverProxy.getDriverProxy(proxyType);
    return this;
  }

  public HostDetails setHostUrl(String hostUrl) {
    this.hostUrl = hostUrl;
    return this;
  }

  public HostDetails setPort(int port) {
    this.port = port;
    return this;
  }

  @Override
  public String toString() {
    return new ToStringer(HostDetails.class)
        .add("host_url", hostUrl)
        .add("port", port)
        .add("proxy_auth_type", proxyType)
        .toString();
  }
}
