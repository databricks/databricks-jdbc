package com.databricks.jdbc.core;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.AUTH_MECH;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.client.jdbc.Driver;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksDataSourceTest {
  @Mock Driver driverMock;
  @Mock DatabricksConnection databricksConnection;

  @Test
  public void testGetUrl() {
    DatabricksDataSource dataSource = new DatabricksDataSource();
    dataSource.setHost("e2-dogfood.staging.cloud.databricks.com");
    dataSource.setPort(443);
    dataSource.setHttpPath("/sql/1.0/warehouses/791ba2a31c7fd70a");
    assertEquals(
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443;httppath=/sql/1.0/warehouses/791ba2a31c7fd70a",
        dataSource.getUrl());
    assertEquals("e2-dogfood.staging.cloud.databricks.com", dataSource.getHost());
    assertEquals(443, dataSource.getPort());
    assertEquals("/sql/1.0/warehouses/791ba2a31c7fd70a", dataSource.getHttpPath());
  }

  @Test
  public void testGetConnection() throws DatabricksSQLException {
    Properties properties = new Properties();
    properties.setProperty(AUTH_MECH, "3");

    DatabricksDataSource dataSource = new DatabricksDataSource(driverMock);
    dataSource.setHost("e2-dogfood.staging.cloud.databricks.com");
    dataSource.setPort(443);
    dataSource.setHttpPath("/sql/1.0/warehouses/791ba2a31c7fd70a");
    dataSource.setProperties(properties);
    dataSource.setUsername("user");
    dataSource.setPassword("password");

    Mockito.when(driverMock.connect(dataSource.getUrl(), properties))
        .thenReturn(databricksConnection);
    Connection connection = dataSource.getConnection();
    assertNotNull(connection);
  }

  @Test
  public void testUnsupportedMethods() {
    DatabricksDataSource dataSource = new DatabricksDataSource();
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> dataSource.getLogWriter(),
        "public PrintWriter getLogWriter()");
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> dataSource.setLogWriter(null),
        "public void setLogWriter(PrintWriter out)");
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> dataSource.getParentLogger(),
        "public Logger getParentLogger()");
  }

  @Test
  public void testGettersAndSetters() throws SQLException {
    Properties properties = new Properties();
    properties.setProperty(AUTH_MECH, "3");

    DatabricksDataSource dataSource = new DatabricksDataSource();
    dataSource.setProperties(properties);
    assertEquals(properties, dataSource.getProperties());

    dataSource.setLoginTimeout(100);
    assertEquals(100, dataSource.getLoginTimeout());

    assertNull(dataSource.unwrap(null));
    assertFalse(dataSource.isWrapperFor(null));
  }
}
