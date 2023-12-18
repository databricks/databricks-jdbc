package com.databricks.jdbc.pooling;

import com.databricks.jdbc.core.DatabricksDataSource;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksConnectionPoolDataSource extends DatabricksDataSource
    implements ConnectionPoolDataSource {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DatabricksConnectionPoolDataSource.class);

  @Override
  public PooledConnection getPooledConnection() {
    LOGGER.debug("public PooledConnection getPooledConnection()");
    return new DatabricksPooledConnection(super.getConnection());
  }

  @Override
  public PooledConnection getPooledConnection(String user, String password) {
    LOGGER.debug(
        "public PooledConnection getPooledConnection(String user = {}, String password = {})",
        user,
        password);
    return new DatabricksPooledConnection(super.getConnection(user, password));
  }
}
