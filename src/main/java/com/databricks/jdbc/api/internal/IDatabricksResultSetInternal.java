package com.databricks.jdbc.api.internal;

import java.io.IOException;
import java.sql.SQLException;
import org.apache.http.HttpEntity;
import org.apache.http.entity.InputStreamEntity;

/** Extended callback handle for java.sql.ResultSet interface */
public interface IDatabricksResultSetInternal {

  void setVolumeOperationEntityStream(HttpEntity httpEntity) throws SQLException, IOException;

  InputStreamEntity getVolumeOperationInputStream() throws SQLException;
}
