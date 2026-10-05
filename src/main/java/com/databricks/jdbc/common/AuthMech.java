package com.databricks.jdbc.common;

import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

public enum AuthMech {
  OTHER,
  PAT,
  OAUTH;

  public static AuthMech parseAuthMech(String authMech) {
    int authMechValue = parseAuthMechValue(authMech);
    AuthMech mech = fromValue(authMechValue);
    if (mech == null) {
      throw new DatabricksDriverException(
          String.format("Does not support authMech value %s", authMech),
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }
    return mech;
  }

  /**
   * Returns the {@link AuthMech} for a numeric value, or {@code null} if unsupported. Single source
   * of truth for supported AuthMech values.
   */
  public static AuthMech fromValue(int authMechValue) {
    switch (authMechValue) {
      case 3:
        return AuthMech.PAT;
      case 11:
        return AuthMech.OAUTH;
      default:
        return null;
    }
  }

  private static int parseAuthMechValue(String authMech) {
    try {
      return Integer.parseInt(authMech);
    } catch (NumberFormatException e) {
      throw new DatabricksDriverException(
          String.format("AuthMech value must be an integer only, and not %s", authMech),
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }
  }
}
