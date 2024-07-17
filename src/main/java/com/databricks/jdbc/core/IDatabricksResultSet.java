package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.StatementStatus;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import org.apache.http.HttpEntity;

public interface IDatabricksResultSet {
  String statementId();

  StatementStatus getStatementStatus();

  long getUpdateCount() throws SQLException;

  boolean hasUpdateCount() throws SQLException;

  void setVolumeOperationEntityStream(HttpEntity httpEntity) throws SQLException, IOException;

  InputStream getVolumeOperationInputStream() throws SQLException;
}
