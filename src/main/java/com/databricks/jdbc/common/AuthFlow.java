package com.databricks.jdbc.common;

public enum AuthFlow {
  TOKEN_PASSTHROUGH,
  CLIENT_CREDENTIALS,
  BROWSER_BASED_AUTHENTICATION,
  AZURE_MANAGED_IDENTITIES;

  public static AuthFlow fromValue(int authFlowValue) {
    AuthFlow[] authFlows = values();
    return authFlowValue >= 0 && authFlowValue < authFlows.length ? authFlows[authFlowValue] : null;
  }
}
