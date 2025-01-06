package com.databricks.jdbc.common;

public enum AuthMech {
  OTHER,
  PAT,
  OAUTH;

  public static AuthMech parseAuthMech(String authMech) {
    int authMechValue = Integer.parseInt(authMech);
    switch (authMechValue) {
      case 3:
        return AuthMech.PAT;
      case 11:
        return AuthMech.OAUTH;
      default:
        throw new UnsupportedOperationException();
    }
  }
}
