package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.EMPTY_STRING;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.sdk.core.DatabricksConfig;
import java.util.Arrays;
import java.util.stream.Collectors;

public class ClientUtils {
  public static DatabricksConfig generateDatabricksConfig(
      IDatabricksConnectionContext connectionContext) throws DatabricksParsingException {
    DatabricksConfig databricksConfig =
        new DatabricksConfig().setUseSystemPropertiesHttp(connectionContext.getUseSystemProxy());
    // Setup proxy settings
    if (connectionContext.getUseProxy()) {
      databricksConfig
          .setProxyHost(connectionContext.getProxyHost())
          .setProxyPort(connectionContext.getProxyPort());
    }
    databricksConfig
        .setProxyAuthType(connectionContext.getProxyAuthType())
        .setProxyUsername(connectionContext.getProxyUser())
        .setProxyPassword(connectionContext.getProxyPassword())
        .setNonProxyHosts(
            convertNonProxyHostConfigToBeSystemPropertyCompliant(
                connectionContext.getNonProxyHosts()));
    return databricksConfig;
  }

  /**
   * Currently, the ODBC driver takes in nonProxyHosts as a comma separated list of suffix of
   * non-proxy hosts i.e. suffix1|suffix2|suffix3. Whereas, the SDK takes in nonProxyHosts as a list
   * of patterns separated by '|'. This pattern conforms to the system property format in the Java
   * Proxy Guide.
   *
   * @param nonProxyHosts Comma separated list of suffix of non-proxy hosts
   * @return nonProxyHosts in system property compliant format from <a
   *     href="https://docs.oracle.com/javase/8/docs/technotes/guides/net/proxies.html">Java Proxy
   *     Guide</a>
   */
  public static String convertNonProxyHostConfigToBeSystemPropertyCompliant(String nonProxyHosts) {
    if (nonProxyHosts == null || nonProxyHosts.isEmpty()) {
      return EMPTY_STRING;
    }
    return Arrays.stream(nonProxyHosts.split(","))
        .map(suffix -> "*" + suffix)
        .collect(Collectors.joining("|"));
  }
}
