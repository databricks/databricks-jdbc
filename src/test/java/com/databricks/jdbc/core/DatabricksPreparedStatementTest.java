package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.DatabricksSdkClient;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksPreparedStatementTest {

  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final String STATEMENT =
      "SELECT * FROM orders WHERE user_id = ? AND shard = ? AND region_code = ? AND namespace = ?";
  private static final String JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";

  @Mock DatabricksResultSet resultSet;

  @Mock DatabricksSdkClient client;

  @Test
  public void testExecuteStatement() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksPreparedStatement statement = new DatabricksPreparedStatement(connection, STATEMENT);
    statement.setLong(1, (long) 100);
    statement.setShort(2, (short) 10);
    statement.setByte(3, (byte) 15);
    statement.setString(4, "value");

    HashMap<Integer, ImmutableSqlParameter> sqlParams =
        new HashMap<>() {
          {
            put(1, getSqlParam(1, 100, DatabricksTypes.BIGINT));
            put(2, getSqlParam(2, (short) 10, DatabricksTypes.SMALLINT));
            put(3, getSqlParam(3, (byte) 15, DatabricksTypes.TINYINT));
            put(4, getSqlParam(4, "value", DatabricksTypes.STRING));
          }
        };
    when(client.executeStatement(
            eq(STATEMENT),
            eq(WAREHOUSE_ID),
            any(HashMap.class),
            eq(StatementType.QUERY),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);

    DatabricksResultSet newResultSet = (DatabricksResultSet) statement.executeQuery();
    assertFalse(statement.isClosed());
    assertEquals(resultSet, newResultSet);
    statement.close();
    assertTrue(statement.isClosed());
  }

  @Test
  public void testExecuteUpdateStatement() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksPreparedStatement statement = new DatabricksPreparedStatement(connection, STATEMENT);

    when(resultSet.getUpdateCount()).thenReturn(2L);
    when(client.executeStatement(
            eq(STATEMENT),
            eq(WAREHOUSE_ID),
            eq(new HashMap<Integer, ImmutableSqlParameter>()),
            eq(StatementType.UPDATE),
            any(IDatabricksSession.class),
            eq(statement)))
        .thenReturn(resultSet);
    int updateCount = statement.executeUpdate();
    assertEquals(2, updateCount);
    assertFalse(statement.isClosed());
    statement.close();
    assertTrue(statement.isClosed());
  }

  private ImmutableSqlParameter getSqlParam(int parameterIndex, Object x, String databricksType) {
    return ImmutableSqlParameter.builder()
        .type(databricksType)
        .value(x)
        .cardinal(parameterIndex)
        .build();
  }
}
