package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.List;
import java.util.Map;

/**
 * Parameter capabilities of an APC SEA session: its CreateSession response and connection options.
 */
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

  /**
   * Native parameters need both server support and no request for client-side interpolation, which
   * callers set with {@code supportManyParameters=1} to bypass the server's parameter-count limit.
   */
  public static boolean supportsNativeParameters(IDatabricksSession session) {
    return !isInterpolationRequested(session)
        && supportsVersion(session, NATIVE_PARAMETERS_VERSION);
  }

  /**
   * Native batches send every set as bound parameters, so they share the parameter-count limit that
   * {@code supportManyParameters=1} exists to avoid.
   */
  public static boolean supportsNativeBatch(IDatabricksSession session) {
    return !isInterpolationRequested(session) && supportsVersion(session, NATIVE_BATCH_VERSION);
  }

  /**
   * Whether the driver is interpolating only because the server cannot take native parameters.
   * Interpolation the caller asked for is not a downgrade.
   */
  public static boolean isParameterDowngrade(IDatabricksSession session) {
    return isApcSea(session)
        && !isInterpolationRequested(session)
        && !supportsVersion(session, NATIVE_PARAMETERS_VERSION);
  }

  public static String downgradeReason(IDatabricksSession session) {
    if (isInterpolationRequested(session)) {
      return "supportManyParameters";
    }
    ImmutableSessionInfo info = session.getSessionInfo();
    if (info == null || info.negotiatedProtocolVersion() == null) {
      return "missing version";
    }
    return "below V8";
  }

  private static boolean isInterpolationRequested(IDatabricksSession session) {
    IDatabricksConnectionContext context = session == null ? null : session.getConnectionContext();
    return context != null && context.supportManyParameters();
  }

  private static boolean supportsVersion(IDatabricksSession session, long minimum) {
    ImmutableSessionInfo info = session == null ? null : session.getSessionInfo();
    Long version = info == null ? null : info.negotiatedProtocolVersion();
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
          || type == ColumnInfoTypeName.USER_DEFINED_TYPE
          || type == ColumnInfoTypeName.VARIANT) {
        throw new DatabricksSQLException(
            "APC native parameters do not support complex logical type " + type,
            DatabricksDriverErrorCode.UNSUPPORTED_APC_PARAMETER.name(),
            DatabricksDriverErrorCode.UNSUPPORTED_APC_PARAMETER);
      }
    }
  }

  /**
   * Whether every set can use the same SQL-level unhex wrapper for binary placeholders. Binding
   * count mismatches are left to binding validation.
   */
  public static boolean hasConsistentBinaryPositions(List<BatchParameterSet> sets) {
    if (sets.isEmpty()) {
      return true;
    }
    Map<Integer, ImmutableSqlParameter> first = sets.get(0).getParameterBindings();
    for (BatchParameterSet set : sets) {
      Map<Integer, ImmutableSqlParameter> current = set.getParameterBindings();
      for (Map.Entry<Integer, ImmutableSqlParameter> entry : first.entrySet()) {
        ImmutableSqlParameter other = current.get(entry.getKey());
        if (other != null && isBinaryBytes(entry.getValue()) != isBinaryBytes(other)) {
          return false;
        }
      }
    }
    return true;
  }

  private static boolean isBinaryBytes(ImmutableSqlParameter parameter) {
    return parameter.type() == ColumnInfoTypeName.BINARY && parameter.value() instanceof byte[];
  }
}
