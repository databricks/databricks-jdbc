package com.databricks.jdbc.core;

import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Statement;

/** Interface for Databricks specific statement. */
public interface IDatabricksStatement {

  /** Returns the underlying session-Id for the statement. */
  String getSessionId();

  void close(boolean removeFromSession) throws SQLException;

  void handleResultSetClose(IDatabricksResultSet resultSet) throws SQLException;

  int getMaxRows() throws SQLException;

  void setStatementId(String statementId);

  String getStatementId();

  Statement getStatement();

  void allowInputStreamForVolumeOperation(boolean allowedInputStream) throws DatabricksSQLException;

  boolean isAllowedInputStreamForVolumeOperation() throws DatabricksSQLException;

  void setInputStreamForUCVolume(InputStream inputStream) throws DatabricksSQLException;

  InputStream getInputStreamForUCVolume() throws DatabricksSQLException;
}
