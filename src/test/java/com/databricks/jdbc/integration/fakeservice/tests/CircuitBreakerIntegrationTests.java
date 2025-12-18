package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_ROW_LIMIT_PER_BLOCK;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.common.SeaCircuitBreakerManager;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for circuit breaker functionality.
 *
 * <p>These tests verify that when the circuit breaker is open, connections use Thrift instead of
 * SEA, demonstrating the integration of circuit breaker state with connection creation logic.
 *
 * <p><b>IMPORTANT:</b> These tests must be run with {@code FAKE_SERVICE_TYPE=THRIFT_SERVER}
 * environment variable to properly simulate the Thrift server:
 *
 * <pre>{@code
 * FAKE_SERVICE_TYPE=THRIFT_SERVER mvn test -Dtest=CircuitBreakerIntegrationTests
 * }</pre>
 */
public class CircuitBreakerIntegrationTests extends AbstractFakeServiceIntegrationTests {

  @BeforeEach
  void setUp() {
    // Reset circuit breaker before each test
    SeaCircuitBreakerManager.reset();
  }

  @AfterEach
  void tearDown() {
    // Clean up circuit breaker state after each test
    SeaCircuitBreakerManager.reset();
  }

  @Test
  void testThriftWithCircuitOpen() throws SQLException {
    // Open the circuit breaker by recording a 429 failure
    SeaCircuitBreakerManager.record429Failure();

    // Verify circuit is open
    assertTrue(SeaCircuitBreakerManager.isCircuitOpen());
    assertTrue(SeaCircuitBreakerManager.getTimeRemainingMs() > 0);

    // Create a new connection - should use Thrift directly due to open circuit
    // IMPORTANT: Don't set USE_THRIFT_CLIENT parameter, let circuit breaker decide
    Connection connection = null;
    try {
      connection = getConnectionWithoutThriftClientParam();

      assertNotNull(connection);
      assertFalse(connection.isClosed());

      // Verify it used Thrift client (forced by circuit breaker)
      DatabricksConnection dbConnection = (DatabricksConnection) connection;
      assertEquals(
          DatabricksClientType.THRIFT, dbConnection.getConnectionContext().getClientType());

      // Circuit should still be open
      assertTrue(SeaCircuitBreakerManager.isCircuitOpen());

    } finally {
      if (connection != null && !connection.isClosed()) {
        connection.close();
      }
    }
  }

  /**
   * Helper method to get a connection without setting USE_THRIFT_CLIENT parameter. This allows the
   * circuit breaker logic to naturally determine the client type.
   */
  private Connection getConnectionWithoutThriftClientParam() throws SQLException {
    Properties connectionProperties = new Properties();
    connectionProperties.put(DatabricksJdbcUrlParams.UID.getParamName(), getDatabricksUser());
    connectionProperties.put(DatabricksJdbcUrlParams.PASSWORD.getParamName(), getDatabricksToken());
    connectionProperties.put(
        DatabricksJdbcUrlParams.ENABLE_SQL_EXEC_HYBRID_RESULTS.getParamName(), "0");
    connectionProperties.put(
        DatabricksJdbcUrlParams.OIDC_DISCOVERY_MODE.getParamName(),
        "0"); // Disable OIDC discovery for fake service tests

    if (DriverUtil.isRunningAgainstFake()) {
      connectionProperties.putIfAbsent(
          DatabricksJdbcUrlParams.CONN_CATALOG.getParamName(),
          FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CONN_CATALOG.getParamName()));
      connectionProperties.putIfAbsent(
          DatabricksJdbcUrlParams.CONN_SCHEMA.getParamName(),
          FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CONN_SCHEMA.getParamName()));
      // DON'T set USE_THRIFT_CLIENT - let circuit breaker decide
      connectionProperties.putIfAbsent(
          DatabricksJdbcUrlParams.ROWS_FETCHED_PER_BLOCK.getParamName(),
          DEFAULT_ROW_LIMIT_PER_BLOCK);

      return DriverManager.getConnection(getFakeServiceJDBCUrl(), connectionProperties);
    }

    return DriverManager.getConnection(getJDBCUrl(), connectionProperties);
  }

  @Test
  void testCircuitRemainsOpen() throws SQLException {
    // Open circuit breaker
    SeaCircuitBreakerManager.record429Failure();
    assertTrue(SeaCircuitBreakerManager.isCircuitOpen());

    Connection connection1 = null;
    Connection connection2 = null;
    try {
      // First connection - should use Thrift
      connection1 = getConnectionWithoutThriftClientParam();
      DatabricksConnection dbConnection1 = (DatabricksConnection) connection1;
      assertEquals(
          DatabricksClientType.THRIFT, dbConnection1.getConnectionContext().getClientType());

      // Circuit should remain open after first connection
      assertTrue(SeaCircuitBreakerManager.isCircuitOpen());

      // Second connection - should also use Thrift (circuit still open)
      connection2 = getConnectionWithoutThriftClientParam();
      DatabricksConnection dbConnection2 = (DatabricksConnection) connection2;
      assertEquals(
          DatabricksClientType.THRIFT, dbConnection2.getConnectionContext().getClientType());

      // Circuit should still be open
      assertTrue(SeaCircuitBreakerManager.isCircuitOpen());

    } finally {
      if (connection1 != null && !connection1.isClosed()) {
        connection1.close();
      }
      if (connection2 != null && !connection2.isClosed()) {
        connection2.close();
      }
    }
  }

  @Test
  void testMultipleConnectionsWithCircuitOpen() throws SQLException {
    // Open circuit breaker
    SeaCircuitBreakerManager.record429Failure();
    assertTrue(SeaCircuitBreakerManager.isCircuitOpen());

    // Create multiple connections - all should use Thrift
    Connection connection1 = null;
    Connection connection2 = null;
    try {
      connection1 = getConnectionWithoutThriftClientParam();
      connection2 = getConnectionWithoutThriftClientParam();

      DatabricksConnection dbConnection1 = (DatabricksConnection) connection1;
      DatabricksConnection dbConnection2 = (DatabricksConnection) connection2;

      // Both should use Thrift due to shared JVM-wide circuit breaker
      assertEquals(
          DatabricksClientType.THRIFT, dbConnection1.getConnectionContext().getClientType());
      assertEquals(
          DatabricksClientType.THRIFT, dbConnection2.getConnectionContext().getClientType());

      // Circuit breaker state should persist
      assertTrue(SeaCircuitBreakerManager.isCircuitOpen());

    } finally {
      if (connection1 != null && !connection1.isClosed()) {
        connection1.close();
      }
      if (connection2 != null && !connection2.isClosed()) {
        connection2.close();
      }
    }
  }
}
