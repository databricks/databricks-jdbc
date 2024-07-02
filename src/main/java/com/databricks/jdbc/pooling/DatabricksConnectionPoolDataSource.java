package com.databricks.jdbc.pooling;

import com.databricks.jdbc.core.DatabricksDataSource;
import com.databricks.jdbc.core.DatabricksSQLException;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

public class DatabricksConnectionPoolDataSource extends DatabricksDataSource
    implements ConnectionPoolDataSource {

  @Override
  public PooledConnection getPooledConnection() throws DatabricksSQLException {
    // LOGGER.debug("public PooledConnection getPooledConnection()");
    return new DatabricksPooledConnection(super.getConnection());
  }

  @Override
  public PooledConnection getPooledConnection(String user, String password)
      throws DatabricksSQLException {
    /* LOGGER.debug(
    "public PooledConnection getPooledConnection(String user = {}, String password = {})",
    user,
    password);*/
    return new DatabricksPooledConnection(super.getConnection(user, password));
  }
}
