package com.databricks.jdbc.api.callback;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.http.entity.InputStreamEntity;

/** Extended callback handle for java.sql.Statement interface */
public interface IDatabricksStatementHandle {
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
