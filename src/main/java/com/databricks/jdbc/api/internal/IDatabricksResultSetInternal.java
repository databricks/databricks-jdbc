package com.databricks.jdbc.api.internal;

import java.sql.SQLException;
import org.apache.http.entity.InputStreamEntity;

/** Extended callback handle for java.sql.ResultSet interface */
public interface IDatabricksResultSetInternal {

  InputStreamEntity getVolumeOperationInputStream() throws SQLException;
}
