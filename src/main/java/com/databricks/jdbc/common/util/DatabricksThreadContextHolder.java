package com.databricks.jdbc.common.util;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.sdk.core.DatabricksConfig;

public class DatabricksThreadContextHolder {
  private static final ThreadLocal<IDatabricksConnectionContext> localConnectionContext =
      new ThreadLocal<>();
  private static final ThreadLocal<DatabricksConfig> localDatabricksConfig = new ThreadLocal<>();
  private static final ThreadLocal<StatementId> localStatementId = new ThreadLocal<>();
  private static final ThreadLocal<Long> localChunkId = new ThreadLocal<>();
  private static final ThreadLocal<Integer> localRetryCount = new ThreadLocal<>();
  private static final ThreadLocal<StatementType> localStatementType = new ThreadLocal<>();

  public static void setConnectionContext(IDatabricksConnectionContext context) {
    localConnectionContext.set(context);
  }

  public static IDatabricksConnectionContext getConnectionContext() {
    return localConnectionContext.get();
  }

  public static void setDatabricksConfig(DatabricksConfig databricksConfig) {
    localDatabricksConfig.set(databricksConfig);
  }

  public static DatabricksConfig getDatabricksConfig() {
    return localDatabricksConfig.get();
  }

  public static void setStatementId(StatementId statementId) {
    localStatementId.set(statementId);
  }

  public static StatementId getStatementId() {
    return localStatementId.get();
  }

  public static void setStatementType(StatementType statementType) {
    localStatementType.set(statementType);
  }

  public static Integer getRetryCount() {
    return localRetryCount.get();
  }

  public static void setRetryCount(Integer retryCount) {
    localRetryCount.set(retryCount);
  }

  public static StatementType getStatementType() {
    return localStatementType.get();
  }

  public static void setChunkId(Long chunkId) {
    localChunkId.set(chunkId);
  }

  public static Long getChunkId() {
    return localChunkId.get();
  }

  public static void clearConnectionContext() {
    localConnectionContext.remove();
  }

  public static void clearStatementInfo() {
    localStatementId.remove();
    localChunkId.remove();
    localStatementType.remove();
    localRetryCount.remove();
  }

  public static void clearAllContext() {
    clearStatementInfo();
    clearConnectionContext();
  }
}
