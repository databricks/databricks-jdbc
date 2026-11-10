package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DatabricksCteStatementTest {
  private static final String JDBC_URL =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:443/default;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;UseThriftClient=0;EnableTelemetry=0;EnableHeartbeat=0;";
  private static final String CTE_INSERT =
      "WITH src AS (SELECT 1 AS id) INSERT INTO target SELECT id FROM src";
  private static final String CTE_SELECT = "WITH src AS (SELECT 1 AS id) SELECT id FROM src";

  @Mock private DatabricksSdkClient client;
  @Mock private DatabricksResultSet resultSet;
  private DatabricksConnection connection;

  @AfterEach
  void closeConnection() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        CTE_INSERT,
        "WITH src (SELECT 1 AS id) INSERT INTO target SELECT id FROM src",
        "WITH src(id) (SELECT 1) INSERT INTO target SELECT id FROM src",
        "WITH src(id) AS (VALUES (1)) INSERT INTO target SELECT id FROM src",
        "WITH src(id) MAX RECURSION LEVEL :limit AS (VALUES (1)) INSERT INTO target SELECT id FROM src",
        "WITH a AS (SELECT 1 AS id), b(id) AS (SELECT id FROM a) INSERT INTO target SELECT id FROM b",
        "WITH a AS (WITH b AS (SELECT 1 AS id) SELECT id FROM b) INSERT INTO target SELECT id FROM a",
        "WITH RECURSIVE a AS (SELECT 1 UNION ALL SELECT id + 1 FROM a WHERE id < 3) INSERT INTO target SELECT * FROM a",
        "WITH `a)`(`id)`) AS (SELECT 1) INSERT INTO target SELECT * FROM `a)`",
        "WITH a AS (SELECT ') UPDATE target' AS txt) INSERT INTO target SELECT txt FROM a",
        "WITH a AS (SELECT 'it''s ) INSERT' AS txt) INSERT INTO target SELECT txt FROM a",
        "WITH a AS (SELECT r'path\\' AS txt) UPDATE target SET id = 2",
        "WITH a AS (SELECT \"column)\" FROM source) INSERT INTO target SELECT * FROM a",
        "/* prefix */ wiTh a AS /* body */ (SELECT /* ( /* nested */ ) */ 1) -- main\nINSERT INTO target SELECT * FROM a",
        "WITH a AS (SELECT 1 AS id) UPDATE target SET id = 2 WHERE id IN (SELECT id FROM a)",
        "WITH a AS (SELECT 1 AS id) DELETE FROM target WHERE id IN (SELECT id FROM a)",
        "WITH a AS (SELECT 1 AS id) MERGE INTO target t USING a ON t.id = a.id WHEN MATCHED THEN DELETE",
        "WITH AS(id) AS (VALUES (1)) INSERT INTO target SELECT id FROM AS",
        "WITH TABLE AS (SELECT 1 AS id) UPDATE target SET id = 2"
      })
  void classifiesCteDmlAsUpdateCount(String sql) {
    assertFalse(DatabricksStatement.shouldReturnResultSet(sql, Collections.emptyList()));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        CTE_SELECT,
        "WITH UPDATE AS (SELECT 1 AS id) SELECT id FROM UPDATE",
        "WITH a(id) AS (VALUES (1)), b AS (SELECT * FROM a) SELECT * FROM b",
        "WITH a AS (SELECT 'INSERT INTO target' AS txt) SELECT txt FROM a",
        "WITH a AS (SELECT 'it\\'s ) DELETE FROM target' AS txt) SELECT txt FROM a",
        "WITH a AS (SELECT r'x''it\\'s ) DELETE FROM target' AS txt) SELECT txt FROM a",
        "WITH a AS (SELECT 1 -- \\\n) UPDATE target\n) SELECT * FROM a",
        "WITH a AS (SELECT 1) SELECT 'DELETE FROM target' FROM a",
        "WITH a AS (SELECT 1) (SELECT * FROM a)",
        "WITH a AS (SELECT 1) VALUES (2)",
        "WITH a AS (SELECT 1) TABLE a",
        "WITH a AS (SELECT 1) FROM a SELECT *",
        "WITH a AS (SELECT 1",
        "WITH a AS (SELECT 1)"
      })
  void preservesCteQueryClassification(String sql) {
    assertTrue(DatabricksStatement.shouldReturnResultSet(sql, Collections.emptyList()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"WITH", "WITH SRC"})
  void honorsOriginalQueryPrefixOverride(String prefix) {
    assertTrue(
        DatabricksStatement.shouldReturnResultSet(CTE_INSERT, Collections.singletonList(prefix)));
    assertFalse(
        DatabricksStatement.shouldReturnResultSet(CTE_INSERT, Collections.singletonList("INSERT")));
  }

  static Stream<Arguments> dmlExecutions() {
    return Stream.of(false, true)
        .flatMap(
            prepared ->
                Stream.of("update", "largeUpdate", "execute")
                    .flatMap(
                        api -> Stream.of(0L, 1L).map(count -> Arguments.of(prepared, api, count))));
  }

  @ParameterizedTest
  @MethodSource("dmlExecutions")
  void exposesCteUpdateCounts(boolean prepared, String api, long count) throws Exception {
    try (Statement statement = statement(prepared, CTE_INSERT, "")) {
      when(resultSet.getUpdateCount()).thenReturn(count);
      switch (api) {
        case "update":
          assertEquals(
              count,
              prepared
                  ? ((PreparedStatement) statement).executeUpdate()
                  : statement.executeUpdate(CTE_INSERT));
          break;
        case "largeUpdate":
          assertEquals(
              count,
              prepared
                  ? ((PreparedStatement) statement).executeLargeUpdate()
                  : statement.executeLargeUpdate(CTE_INSERT));
          break;
        case "execute":
          assertFalse(
              prepared ? ((PreparedStatement) statement).execute() : statement.execute(CTE_INSERT));
          break;
        default:
          fail("Unexpected API: " + api);
      }
      assertEquals(count, statement.getUpdateCount());
      assertEquals(count, statement.getLargeUpdateCount());
      assertNull(statement.getResultSet());
      verifyExecution(
          statement, CTE_INSERT, api.equals("execute") ? StatementType.SQL : StatementType.UPDATE);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsCteDmlInExecuteQuery(boolean prepared) throws Exception {
    try (Statement statement = statement(prepared, CTE_INSERT, "")) {
      SQLException error =
          assertThrows(
              DatabricksSQLException.class,
              () -> {
                if (prepared) {
                  ((PreparedStatement) statement).executeQuery();
                } else {
                  statement.executeQuery(CTE_INSERT);
                }
              });
      assertEquals("RESULT_SET_ERROR", error.getSQLState());
      assertEquals(0, error.getErrorCode());
      assertTrue(error.getMessage().contains("execution was successful"));
      verifyExecution(statement, CTE_INSERT, StatementType.QUERY);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void returnsCteSelectResultSet(boolean prepared) throws Exception {
    try (Statement statement = statement(prepared, CTE_SELECT, "")) {
      assertSame(
          resultSet,
          prepared
              ? ((PreparedStatement) statement).executeQuery()
              : statement.executeQuery(CTE_SELECT));
      assertEquals(-1, statement.getUpdateCount());
      verifyExecution(statement, CTE_SELECT, StatementType.QUERY);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void optOutReturnsCteDmlResultSet(boolean prepared) throws Exception {
    try (Statement statement = statement(prepared, CTE_INSERT, "NonRowcountQueryPrefixes=WITH;")) {
      assertSame(
          resultSet,
          prepared
              ? ((PreparedStatement) statement).executeQuery()
              : statement.executeQuery(CTE_INSERT));
      assertSame(resultSet, statement.getResultSet());
      assertEquals(-1, statement.getUpdateCount());
      verifyExecution(statement, CTE_INSERT, StatementType.QUERY);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void optOutExecuteExposesCteDmlResultSet(boolean prepared) throws Exception {
    try (Statement statement = statement(prepared, CTE_INSERT, "NonRowcountQueryPrefixes=WITH;")) {
      assertTrue(
          prepared ? ((PreparedStatement) statement).execute() : statement.execute(CTE_INSERT));
      assertSame(resultSet, statement.getResultSet());
      assertEquals(-1, statement.getUpdateCount());
      verifyExecution(statement, CTE_INSERT, StatementType.SQL);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void optOutPreservesUpdateApiRejection(boolean prepared) throws Exception {
    try (Statement statement = statement(prepared, CTE_INSERT, "NonRowcountQueryPrefixes=WITH;")) {
      SQLException error =
          assertThrows(
              DatabricksSQLException.class,
              () -> {
                if (prepared) {
                  ((PreparedStatement) statement).executeUpdate();
                } else {
                  statement.executeUpdate(CTE_INSERT);
                }
              });
      assertEquals("RESULT_SET_ERROR", error.getSQLState());
      assertEquals(0, error.getErrorCode());
      verifyExecution(statement, CTE_INSERT, StatementType.UPDATE);
    }
  }

  private Statement statement(boolean prepared, String sql, String options) throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL + options, new Properties());
    connection = new DatabricksConnection(context, client);
    Statement statement =
        prepared
            ? new DatabricksPreparedStatement(connection, sql)
            : new DatabricksStatement(connection);
    when(client.executeStatement(
            eq(sql),
            eq(new Warehouse("99999999")),
            anyMap(),
            any(StatementType.class),
            any(IDatabricksSession.class),
            eq((DatabricksStatement) statement),
            any()))
        .thenReturn(resultSet);
    return statement;
  }

  private void verifyExecution(Statement statement, String sql, StatementType type)
      throws Exception {
    verify(client)
        .executeStatement(
            eq(sql),
            eq(new Warehouse("99999999")),
            anyMap(),
            eq(type),
            any(IDatabricksSession.class),
            eq((DatabricksStatement) statement),
            any());
  }
}
