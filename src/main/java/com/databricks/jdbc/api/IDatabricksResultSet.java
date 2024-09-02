package com.databricks.jdbc.api;

import com.databricks.sdk.service.sql.StatementStatus;
import java.io.IOException;
import java.sql.SQLException;
import org.apache.http.HttpEntity;
import org.apache.http.entity.InputStreamEntity;

public interface IDatabricksResultSet {
  String statementId();

  StatementStatus getStatementStatus();

  long getUpdateCount() throws SQLException;

  boolean hasUpdateCount() throws SQLException;

  void setVolumeOperationEntityStream(HttpEntity httpEntity) throws SQLException, IOException;

  InputStreamEntity getVolumeOperationInputStream() throws SQLException;
}
