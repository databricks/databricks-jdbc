package com.databricks.jdbc.api.internal;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.http.entity.InputStreamEntity;

/** Extended callback handle for java.sql.Statement interface */
public interface IDatabricksStatementInternal {

  void close(boolean removeFromSession) throws DatabricksSQLException;

  void handleResultSetClose(IDatabricksResultSet resultSet) throws DatabricksSQLException;

  int getMaxRows() throws SQLException;

  void setStatementId(StatementId statementId);

  String getStatementId();

  Statement getStatement();

  void allowInputStreamForVolumeOperation(boolean allowedInputStream) throws DatabricksSQLException;

  boolean isAllowedInputStreamForVolumeOperation() throws DatabricksSQLException;

  void setInputStreamForUCVolume(InputStreamEntity inputStream) throws DatabricksSQLException;

  InputStreamEntity getInputStreamForUCVolume() throws DatabricksSQLException;
}
