package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.sdk.core.DatabricksException;
import org.apache.http.client.utils.URIBuilder;

import java.net.URISyntaxException;

public class AuthUtils {
  public static String getTokenEndpoint(IDatabricksConnectionContext context) {
    String tokenUrl;
    if (context.getOAuth2TokenEndpoint() != null) {
      tokenUrl = context.getOAuth2TokenEndpoint();
    } else {
      try {
        tokenUrl =
                new URIBuilder().setHost(context.getHostForOAuth())
                        .setScheme("https")
                        .setPathSegments("oidc", "v1", "token")
                        .build().toString();
      } catch (URISyntaxException e) {
        LoggingUtil.log(LogLevel.ERROR, "Failed to build token url");
        throw new DatabricksException("Failed to build token url", e);
      }
    }
    return tokenUrl;
  }
}
