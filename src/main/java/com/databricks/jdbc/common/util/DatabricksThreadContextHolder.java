package com.databricks.jdbc.common.util;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;

public class DatabricksThreadContextHolder {
  private static final ThreadLocal<IDatabricksConnectionContext> localConnectionContext =
      new ThreadLocal<>();
  private static final ThreadLocal<StatementId> localStatementId = new ThreadLocal<>();
  private static final ThreadLocal<Long> localChunkId = new ThreadLocal<>();
  private static final ThreadLocal<StatementType> localStatementType = new ThreadLocal<>();

  public static void setConnectionContext(IDatabricksConnectionContext context) {
    localConnectionContext.set(context);
  }

  public static IDatabricksConnectionContext getConnectionContext() {
    return localConnectionContext.get();
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
    localStatementId.remove();
  }

  public static void clearStatementInfo() {
    localStatementId.remove();
    localChunkId.remove();
  }

  public static void clearAllContext() {
    localConnectionContext.remove();
    localStatementId.remove();
    localChunkId.remove();
  }
}
