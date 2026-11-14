package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.Map;

/** Capabilities granted only by the current APC SEA CreateSession response. */
public final class ApcParameterCapabilities {
  public static final long NATIVE_PARAMETERS_VERSION = 42248L;
  public static final long NATIVE_BATCH_VERSION = 42250L;

  private ApcParameterCapabilities() {}

  public static boolean isApcSea(IDatabricksSession session) {
    ImmutableSessionInfo info = session == null ? null : session.getSessionInfo();
    return info != null
        && info.clientType() == DatabricksClientType.SEA
        && info.computeResource() instanceof AllPurposeCluster;
  }

  public static boolean supportsNativeParameters(IDatabricksSession session) {
    return supportsVersion(session, NATIVE_PARAMETERS_VERSION);
  }

  public static boolean supportsNativeBatch(IDatabricksSession session) {
    return supportsVersion(session, NATIVE_BATCH_VERSION);
  }

  public static String downgradeReason(IDatabricksSession session) {
    ImmutableSessionInfo info = session.getSessionInfo();
    if (info == null || info.negotiatedProtocolVersion() == null) {
      return "missing version";
    }
    return info.liveProtocolVersion() ? "below V8" : "non-live version";
  }

  private static boolean supportsVersion(IDatabricksSession session, long minimum) {
    ImmutableSessionInfo info = session == null ? null : session.getSessionInfo();
    Long version = info == null ? null : info.nativeProtocolVersion();
    return isApcSea(session) && version != null && version >= minimum;
  }

  public static void rejectComplexBindings(Map<Integer, ImmutableSqlParameter> bindings)
      throws DatabricksSQLException {
    for (ImmutableSqlParameter parameter : bindings.values()) {
      ColumnInfoTypeName type = parameter.type();
      if (type == ColumnInfoTypeName.ARRAY
          || type == ColumnInfoTypeName.MAP
          || type == ColumnInfoTypeName.STRUCT
          || type == ColumnInfoTypeName.UNION
          || type == ColumnInfoTypeName.USER_DEFINED_TYPE) {
        throw new DatabricksSQLException(
            "APC native parameters do not support complex logical type " + type,
            DatabricksDriverErrorCode.UNSUPPORTED_APC_PARAMETER.name(),
            DatabricksDriverErrorCode.UNSUPPORTED_APC_PARAMETER);
      }
    }
  }
}
