package com.databricks.jdbc.api;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.http.entity.InputStreamEntity;

/** Interface for Databricks specific statement. */
public interface IDatabricksStatement {

  /** Returns the underlying session-Id for the statement. */
  String getSessionId();

  void close(boolean removeFromSession) throws DatabricksSQLException;

  void handleResultSetClose(IDatabricksResultSet resultSet) throws DatabricksSQLException;

  int getMaxRows() throws SQLException;

  void setStatementId(String statementId);

  String getStatementId();

  Statement getStatement();

  void allowInputStreamForVolumeOperation(boolean allowedInputStream) throws DatabricksSQLException;

  boolean isAllowedInputStreamForVolumeOperation() throws DatabricksSQLException;

  void setInputStreamForUCVolume(InputStreamEntity inputStream) throws DatabricksSQLException;

  InputStreamEntity getInputStreamForUCVolume() throws DatabricksSQLException;
}
