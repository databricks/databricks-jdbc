package com.databricks.jdbc.api.internal;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.Statement;
import org.apache.http.entity.InputStreamEntity;

/** Extended callback handle for java.sql.Statement interface */
public interface IDatabricksStatementInternal {

  void close(boolean removeFromSession) throws DatabricksSQLException;

  void handleResultSetClose(IDatabricksResultSet resultSet) throws DatabricksSQLException;

  int getMaxRows() throws DatabricksSQLException;

  void setStatementId(StatementId statementId);

  StatementId getStatementId();

  Statement getStatement();

  void allowInputStreamForVolumeOperation(boolean allowedInputStream) throws DatabricksSQLException;

  boolean isAllowedInputStreamForVolumeOperation() throws DatabricksSQLException;

  void setInputStreamForUCVolume(InputStreamEntity inputStream) throws DatabricksSQLException;

  InputStreamEntity getInputStreamForUCVolume() throws DatabricksSQLException;

  /**
   * Marks that the server returned direct (inline) results and closed the operation. The JDBC
   * Statement remains open for re-execution. Default no-op for implementations that don't support
   * direct results.
   */
  default void markDirectResultsReceived() {
    // no-op by default
  }
}
