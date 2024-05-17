package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.jdbc.integration.fakeservice.DatabricksWireMockExtension;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import com.databricks.jdbc.integration.fakeservice.StubMappingCredentialsCleaner;
import com.github.tomakehurst.wiremock.extension.Extension;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Integration tests for concurrent operations on a table. */
public class ConcurrencyIntegrationTests {

  /**
   * {@link FakeServiceExtension} for {@link DatabricksJdbcConstants.FakeServiceType#SQL_EXEC}.
   * Intercepts all requests to SQL Execution API.
   */
  @RegisterExtension
  private static final FakeServiceExtension sqlExecApiExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig().dynamicPort().dynamicHttpsPort().extensions(getExtensions())),
          DatabricksJdbcConstants.FakeServiceType.SQL_EXEC,
          "https://" + System.getenv("DATABRICKS_HOST"));

  /**
   * {@link FakeServiceExtension} for {@link DatabricksJdbcConstants.FakeServiceType#CLOUD_FETCH}.
   * Intercepts all requests to Cloud Fetch API.
   */
  @RegisterExtension
  private static final FakeServiceExtension cloudFetchApiExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig().dynamicPort().dynamicHttpsPort().extensions(getExtensions())),
          DatabricksJdbcConstants.FakeServiceType.CLOUD_FETCH,
          "https://dbstoragepzjc6kojqibtg.blob.core.windows.net");

  private Connection connection;

  private static final String tableName = "concurrency_test_table";

  @BeforeEach
  void setUp() throws SQLException {
    connection = getValidJDBCConnection();
    createTestTable();
  }

  @AfterEach
  void cleanUp() throws SQLException {
    dropTestTable();
    if (connection != null) {
      connection.close();
    }
  }

  private void createTestTable() throws SQLException {
    String sql =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "id INT PRIMARY KEY, "
            + "counter INT"
            + ");";
    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      preparedStatement.execute();
    }
    String insertSQL =
        "INSERT INTO " + getFullyQualifiedTableName(tableName) + " (id, counter) VALUES (1, 0)";
    executeSQL(insertSQL);
  }

  private void dropTestTable() throws SQLException {
    String sql = "DROP TABLE IF EXISTS " + getFullyQualifiedTableName(tableName);
    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      preparedStatement.execute();
    }
  }

  @Test
  void testConcurrentUpdates() throws InterruptedException, SQLException {
    ExecutorService executor = Executors.newFixedThreadPool(2);

    AtomicInteger counter = new AtomicInteger();

    Runnable updateTask =
        () -> {
          String sql =
              "UPDATE "
                  + getFullyQualifiedTableName(tableName)
                  + " SET counter = counter + 1 WHERE id = 1";
          try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.executeUpdate();
          } catch (SQLException e) {
            counter.getAndIncrement();
            System.out.println("Expected exception on concurrent update: " + e.getMessage());
          }
        };

    // Execute concurrent updates
    executor.submit(updateTask);
    executor.submit(updateTask);
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);

    assertEquals(counter.get(), 1);

    String selectSQL =
        "SELECT counter FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1";
    ResultSet rs = executeQuery(selectSQL);
    rs.next();
    String r = rs.getString("counter");
    assertEquals(r, "1");
  }

  @Test
  void testConcurrentReads() throws InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(2);

    Runnable readTask =
        () -> {
          String sql =
              "SELECT counter FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1";
          try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
              ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
              System.out.println("Read counter: " + resultSet.getInt("counter"));
            }
          } catch (SQLException e) {
            fail("Read operation should not fail.");
          }
        };

    // Execute concurrent reads
    executor.submit(readTask);
    executor.submit(readTask);
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);
  }

  /** Returns the extensions to be used for stubbing. */
  private static Extension[] getExtensions() {
    return new Extension[] {new StubMappingCredentialsCleaner()};
  }
}
