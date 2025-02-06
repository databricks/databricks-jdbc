package com.databricks.jdbc.model.telemetry.enums;

import com.databricks.sdk.core.ProxyConfig;

public enum DriverProxy {
  AUTH_UNSPECIFIED,
  NONE,
  BASIC,
  SPNEGO;

  public static DriverProxy getDriverProxy(ProxyConfig.ProxyAuthType proxyAuthType) {
    switch (proxyAuthType) {
      case NONE:
        return NONE;
      case BASIC:
        return BASIC;
      case SPNEGO:
        return SPNEGO;
      default:
        return AUTH_UNSPECIFIED;
    }
  }
}
