package com.databricks.jdbc.common;

import com.databricks.jdbc.exception.DatabricksSQLRuntimeException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

public enum AuthMech {
  OTHER,
  PAT,
  OAUTH;

  public static AuthMech parseAuthMech(String authMech) {
    int authMechValue = parseAuthMechValue(authMech);
    switch (authMechValue) {
      case 3:
        return AuthMech.PAT;
      case 11:
        return AuthMech.OAUTH;
      default:
        throw new DatabricksSQLRuntimeException(
            String.format("Does not support authMech value %s", authMech),
            DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }
  }

  private static int parseAuthMechValue(String authMech) {
    try {
      return Integer.parseInt(authMech);
    } catch (NumberFormatException e) {
      throw new DatabricksSQLRuntimeException(
          String.format("AuthMech value must be an integer only, and not %s", authMech),
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }
  }
}
