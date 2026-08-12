package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Properties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksCallableStatementTest {

  private static final String WAREHOUSE_ID = "99999999";
  private static final String JDBC_URL =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;"
          + "transportMode=http;ssl=1;AuthMech=3;"
          + "httpPath=/sql/1.0/warehouses/99999999;";
  private static final String CALL_SQL = "{call my_proc(?, ?)}";
  // DatabricksCallableStatement enables escape processing, so {call ...} is converted to CALL ...
  private static final String CALL_SQL_AS_EXECUTED = "CALL my_proc(?, ?)";

  @Mock DatabricksResultSet resultSet;
  @Mock DatabricksSdkClient client;

  private static final String WAREHOUSE_ID_VALUE = "99999999";
  private static final ImmutableSessionInfo SESSION_INFO =
      ImmutableSessionInfo.builder()
          .computeResource(new Warehouse(WAREHOUSE_ID_VALUE))
          .sessionId("test-session-id")
          .build();

  private DatabricksConnection createConnection() throws Exception {
    IDatabricksConnectionContext ctx =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    lenient()
        .when(client.createSession(any(Warehouse.class), any(), any(), any(java.util.Map.class)))
        .thenReturn(SESSION_INFO);
    DatabricksConnection conn = new DatabricksConnection(ctx, client);
    conn.open();
    return conn;
  }

  // ===========================================================================
  // Construction tests
  // ===========================================================================

  @Nested
  @DisplayName("Construction tests")
  class ConstructionTests {

    @Test
    @DisplayName("prepareCall creates a DatabricksCallableStatement")
    void testPrepareCallCreatesCallableStatement() throws Exception {
      DatabricksConnection connection = createConnection();

      CallableStatement stmt = connection.prepareCall(CALL_SQL);
      assertNotNull(stmt);
      assertInstanceOf(DatabricksCallableStatement.class, stmt);
      assertInstanceOf(PreparedStatement.class, stmt);
      assertInstanceOf(CallableStatement.class, stmt);
      stmt.close();
    }

    @Test
    @DisplayName("prepareCall with CALL syntax (no JDBC escape) works")
    void testPrepareCallWithNativeSyntax() throws Exception {
      DatabricksConnection connection = createConnection();

      CallableStatement stmt = connection.prepareCall("CALL my_proc(?, ?)");
      assertNotNull(stmt);
      assertInstanceOf(DatabricksCallableStatement.class, stmt);
      stmt.close();
    }

    @Test
    @DisplayName("{? = call ...} return value syntax is rejected")
    void testReturnValueSyntaxThrows() throws Exception {
      DatabricksConnection connection = createConnection();

      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> connection.prepareCall("{? = call my_func()}"));
    }

    @Test
    @DisplayName("{? = call ...} syntax variations are all rejected")
    void testReturnValueSyntaxVariations() throws Exception {
      DatabricksConnection connection = createConnection();

      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> connection.prepareCall("{?= call my_func()}"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> connection.prepareCall("{ ? = call my_func()}"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> connection.prepareCall("{  ?  =  call my_func(?, ?)}"));
    }

    @Test
    @DisplayName("prepareCall with unsupported resultSetType throws")
    void testPrepareCallWithUnsupportedResultSetType() throws Exception {
      DatabricksConnection connection = createConnection();

      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () ->
              connection.prepareCall(
                  CALL_SQL, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
    }

    @Test
    @DisplayName("prepareCall with valid resultSetType/concurrency succeeds")
    void testPrepareCallWithValidResultSetTypeSucceeds() throws Exception {
      DatabricksConnection connection = createConnection();

      CallableStatement stmt =
          connection.prepareCall(CALL_SQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      assertNotNull(stmt);
      assertInstanceOf(DatabricksCallableStatement.class, stmt);
      stmt.close();
    }

    @Test
    @DisplayName("prepareCall with null SQL throws")
    void testPrepareCallWithNullSqlThrows() throws Exception {
      DatabricksConnection connection = createConnection();

      // null SQL is rejected by the parent DatabricksPreparedStatement constructor
      assertThrows(Exception.class, () -> connection.prepareCall(null));
    }

    @Test
    @DisplayName("prepareCall on closed connection throws for 1-arg overload")
    void testPrepareCallOnClosedConnectionThrows1Arg() throws Exception {
      DatabricksConnection connection = createConnection();
      connection.close();

      assertThrows(DatabricksSQLException.class, () -> connection.prepareCall(CALL_SQL));
    }

    @Test
    @DisplayName("prepareCall on closed connection throws for 3-arg overload")
    void testPrepareCallOnClosedConnectionThrows3Arg() throws Exception {
      DatabricksConnection connection = createConnection();
      connection.close();

      assertThrows(
          DatabricksSQLException.class,
          () ->
              connection.prepareCall(
                  CALL_SQL,
                  ResultSet.TYPE_FORWARD_ONLY,
                  ResultSet.CONCUR_READ_ONLY,
                  ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }
  }

  // ===========================================================================
  // IN parameter and execution tests
  // ===========================================================================

  @Nested
  @DisplayName("IN parameter and execution tests")
  class ExecutionTests {

    @Test
    @DisplayName("native CALL syntax (no JDBC escape) works with execute()")
    void testNativeCallSyntaxExecute() throws Exception {
      DatabricksConnection connection = createConnection();
      String nativeCallSql = "CALL my_proc(?, ?)";
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, nativeCallSql);

      stmt.setInt(1, 42);
      stmt.setString(2, "test");

      when(client.executeStatement(
              eq(nativeCallSql),
              eq(new Warehouse(WAREHOUSE_ID)),
              any(HashMap.class),
              eq(StatementType.SQL),
              any(IDatabricksSession.class),
              eq(stmt),
              any()))
          .thenReturn(resultSet);

      // Native CALL syntax is not modified by convertCallEscapeSyntax (no braces)
      // and CALL_PATTERN matches, so shouldReturnResultSet is true
      assertTrue(stmt.execute());
      stmt.close();
    }

    @Test
    @DisplayName("execute() returns true for callable statements (procedures can return results)")
    void testExecuteReturnsTrueForCallable() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      stmt.setInt(1, 42);
      stmt.setString(2, "test");

      when(client.executeStatement(
              eq(CALL_SQL_AS_EXECUTED),
              eq(new Warehouse(WAREHOUSE_ID)),
              any(HashMap.class),
              eq(StatementType.SQL),
              any(IDatabricksSession.class),
              eq(stmt),
              any()))
          .thenReturn(resultSet);

      // shouldReturnResultSet is overridden to true for callable statements
      boolean hasResultSet = stmt.execute();
      assertTrue(hasResultSet);
      stmt.close();
    }

    @Test
    @DisplayName("executeQuery() works for callable statements")
    void testExecuteQueryWorks() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      stmt.setInt(1, 42);
      stmt.setString(2, "test");

      when(client.executeStatement(
              eq(CALL_SQL_AS_EXECUTED),
              eq(new Warehouse(WAREHOUSE_ID)),
              any(HashMap.class),
              eq(StatementType.QUERY),
              any(IDatabricksSession.class),
              eq(stmt),
              any()))
          .thenReturn(resultSet);

      ResultSet rs = stmt.executeQuery();
      assertNotNull(rs);
      assertEquals(resultSet, rs);
      stmt.close();
    }

    @Test
    @DisplayName("executeUpdate() throws for callable statements (they are result-set-returning)")
    void testExecuteUpdateThrowsForCallable() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      // executeUpdate is explicitly not supported for callable statements
      DatabricksSQLFeatureNotSupportedException ex =
          assertThrows(DatabricksSQLFeatureNotSupportedException.class, stmt::executeUpdate);
      assertTrue(ex.getMessage().contains("execute()"));
      stmt.close();
    }

    @Test
    @DisplayName("clearParameters works")
    void testClearParameters() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      stmt.setInt(1, 42);
      stmt.setString(2, "test");
      assertDoesNotThrow(stmt::clearParameters);
      stmt.close();
    }

    @Test
    @DisplayName("addBatch and executeBatch work for callable statement")
    void testBatchExecution() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      stmt.setInt(1, 1);
      stmt.setString(2, "first");
      stmt.addBatch();

      stmt.setInt(1, 2);
      stmt.setString(2, "second");
      stmt.addBatch();

      when(client.executeStatement(
              eq(CALL_SQL_AS_EXECUTED),
              eq(new Warehouse(WAREHOUSE_ID)),
              any(HashMap.class),
              eq(StatementType.UPDATE),
              any(IDatabricksSession.class),
              eq(stmt),
              any()))
          .thenReturn(resultSet);

      // executeBatch uses UPDATE path internally, but batch doesn't check shouldReturnResultSet
      int[] results = stmt.executeBatch();
      assertNotNull(results);
      stmt.close();
    }

    @Test
    @DisplayName("getParameterMetaData correctly counts params in {call ...} syntax")
    void testParameterCountInCallSyntax() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      ParameterMetaData pmd = stmt.getParameterMetaData();
      assertNotNull(pmd);
      assertEquals(2, pmd.getParameterCount());
      stmt.close();
    }

    @Test
    @DisplayName("getParameterMetaData counts params for no-arg procedure")
    void testParameterCountNoArgs() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt =
          new DatabricksCallableStatement(connection, "{call my_proc()}");

      assertEquals(0, stmt.getParameterMetaData().getParameterCount());
      stmt.close();
    }

    @Test
    @DisplayName("{call ...} conversion is independent of escapeProcessing flag")
    void testCallConversionIndependentOfEscapeProcessing() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      // Disable escape processing — {call ...} should still be converted
      stmt.setEscapeProcessing(false);

      // The mock expects CALL_SQL_AS_EXECUTED ("CALL my_proc(?, ?)") because
      // the {call} → CALL conversion happens at construction, not via escape processing
      when(client.executeStatement(
              eq(CALL_SQL_AS_EXECUTED),
              eq(new Warehouse(WAREHOUSE_ID)),
              any(HashMap.class),
              eq(StatementType.SQL),
              any(IDatabricksSession.class),
              eq(stmt),
              any()))
          .thenReturn(resultSet);

      assertTrue(stmt.execute());
      stmt.close();
    }

    @Test
    @DisplayName("getParameterMetaData returns metadata")
    void testGetParameterMetaData() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      ParameterMetaData pmd = stmt.getParameterMetaData();
      assertNotNull(pmd);
      assertEquals(2, pmd.getParameterCount());
      stmt.close();
    }
  }

  // ===========================================================================
  // OUT parameter rejection — exhaustive coverage
  // ===========================================================================

  @Nested
  @DisplayName("OUT parameter rejection tests")
  class OutParameterTests {

    @Test
    @DisplayName("registerOutParameter by index (int-based) throws")
    void testRegisterOutParameterByIndex() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter(1, Types.INTEGER));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter(1, Types.DECIMAL, 2));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter(1, Types.STRUCT, "MY_TYPE"));
      stmt.close();
    }

    @Test
    @DisplayName("registerOutParameter by name (int-based) throws")
    void testRegisterOutParameterByName() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter("param1", Types.INTEGER));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter("param1", Types.DECIMAL, 2));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter("param1", Types.STRUCT, "MY_TYPE"));
      stmt.close();
    }

    @Test
    @DisplayName("registerOutParameter with SQLType by index throws")
    void testRegisterOutParameterSQLTypeByIndex() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter(1, JDBCType.INTEGER));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter(1, JDBCType.DECIMAL, 2));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter(1, JDBCType.STRUCT, "MY_TYPE"));
      stmt.close();
    }

    @Test
    @DisplayName("registerOutParameter with SQLType by name throws")
    void testRegisterOutParameterSQLTypeByName() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter("p", JDBCType.INTEGER));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter("p", JDBCType.DECIMAL, 2));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.registerOutParameter("p", JDBCType.STRUCT, "MY_TYPE"));
      stmt.close();
    }

    @Test
    @DisplayName("wasNull throws")
    void testWasNull() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      assertThrows(DatabricksSQLFeatureNotSupportedException.class, stmt::wasNull);
      stmt.close();
    }

    @Test
    @DisplayName("All getXXX(int) output retrieval methods throw")
    void testAllGetByIndexThrows() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getString(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBoolean(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getByte(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getShort(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getInt(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getLong(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getFloat(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getDouble(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBigDecimal(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBigDecimal(1, 2));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBytes(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getDate(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getTime(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getTimestamp(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getObject(1));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.getObject(1, new HashMap<>()));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getRef(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBlob(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getClob(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getArray(1));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.getDate(1, Calendar.getInstance()));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.getTime(1, Calendar.getInstance()));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.getTimestamp(1, Calendar.getInstance()));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getURL(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getRowId(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getNClob(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getSQLXML(1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getNString(1));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getNCharacterStream(1));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getCharacterStream(1));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getObject(1, String.class));
      stmt.close();
    }

    @Test
    @DisplayName("All getXXX(String) output retrieval methods throw")
    void testAllGetByNameThrows() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getString("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBoolean("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getByte("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getShort("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getInt("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getLong("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getFloat("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getDouble("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBigDecimal("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBytes("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getDate("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getTime("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getTimestamp("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getObject("p"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.getObject("p", new HashMap<>()));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getRef("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getBlob("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getClob("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getArray("p"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.getDate("p", Calendar.getInstance()));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.getTime("p", Calendar.getInstance()));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.getTimestamp("p", Calendar.getInstance()));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getURL("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getRowId("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getNClob("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getSQLXML("p"));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getNString("p"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getNCharacterStream("p"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getCharacterStream("p"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.getObject("p", String.class));
      stmt.close();
    }
  }

  // ===========================================================================
  // Named parameter rejection — exhaustive coverage
  // ===========================================================================

  @Nested
  @DisplayName("Named parameter rejection tests")
  class NamedParameterTests {

    @Test
    @DisplayName("All setXXX(String, value) named parameter methods throw")
    void testAllSetByNameThrows() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      // Basic types
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setNull("p", Types.INTEGER));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setBoolean("p", true));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setByte("p", (byte) 1));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setShort("p", (short) 1));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setInt("p", 42));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setLong("p", 42L));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setFloat("p", 1.0f));
      assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setDouble("p", 1.0));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setBigDecimal("p", BigDecimal.ONE));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setString("p", "val"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setBytes("p", new byte[] {1, 2}));

      // Date/Time types
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setDate("p", new Date(0)));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setTime("p", new Time(0)));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setTimestamp("p", new Timestamp(0)));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setDate("p", new Date(0), Calendar.getInstance()));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setTime("p", new Time(0), Calendar.getInstance()));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setTimestamp("p", new Timestamp(0), Calendar.getInstance()));

      // Stream types
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setAsciiStream("p", new ByteArrayInputStream(new byte[0]), 0));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setBinaryStream("p", new ByteArrayInputStream(new byte[0]), 0));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setCharacterStream("p", new StringReader(""), 0));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setAsciiStream("p", new ByteArrayInputStream(new byte[0]), 0L));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setBinaryStream("p", new ByteArrayInputStream(new byte[0]), 0L));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setCharacterStream("p", new StringReader(""), 0L));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setAsciiStream("p", new ByteArrayInputStream(new byte[0])));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setBinaryStream("p", new ByteArrayInputStream(new byte[0])));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setCharacterStream("p", new StringReader("")));

      // Object types
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setObject("p", "val"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setObject("p", "val", Types.VARCHAR));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setObject("p", "val", Types.VARCHAR, 0));

      // NChar/NClob/Clob/Blob/SQLXML types
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setNull("p", Types.VARCHAR, "VARCHAR"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setNString("p", "val"));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setNCharacterStream("p", new StringReader(""), 0L));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setNCharacterStream("p", new StringReader("")));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setClob("p", new StringReader(""), 0L));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setClob("p", new StringReader("")));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setBlob("p", new ByteArrayInputStream(new byte[0]), 0L));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setBlob("p", new ByteArrayInputStream(new byte[0])));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setNClob("p", new StringReader(""), 0L));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setNClob("p", new StringReader("")));

      // Typed object overloads (Blob, Clob, NClob, SQLXML, RowId, URL)
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setURL("p", (java.net.URL) null));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setRowId("p", (RowId) null));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setNClob("p", (NClob) null));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setSQLXML("p", (SQLXML) null));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setBlob("p", (Blob) null));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class, () -> stmt.setClob("p", (Clob) null));

      // SQLType-based overrides (Java 8+)
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setObject("p", "val", JDBCType.VARCHAR, 0));
      assertThrows(
          DatabricksSQLFeatureNotSupportedException.class,
          () -> stmt.setObject("p", "val", JDBCType.VARCHAR));

      stmt.close();
    }
  }

  // ===========================================================================
  // Exception message tests
  // ===========================================================================

  @Nested
  @DisplayName("Exception message tests")
  class ExceptionMessageTests {

    @Test
    @DisplayName("OUT parameter exception has clear Databricks-specific message")
    void testOutParamExceptionMessage() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      DatabricksSQLFeatureNotSupportedException ex =
          assertThrows(
              DatabricksSQLFeatureNotSupportedException.class,
              () -> stmt.registerOutParameter(1, Types.INTEGER));
      assertTrue(ex.getMessage().contains("OUT and INOUT parameters are not supported"));
      stmt.close();
    }

    @Test
    @DisplayName("SQLType-based registerOutParameter has clear Databricks-specific message")
    void testSQLTypeOutParamExceptionMessage() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      DatabricksSQLFeatureNotSupportedException ex =
          assertThrows(
              DatabricksSQLFeatureNotSupportedException.class,
              () -> stmt.registerOutParameter(1, JDBCType.INTEGER));
      assertTrue(ex.getMessage().contains("OUT and INOUT parameters are not supported"));
      stmt.close();
    }

    @Test
    @DisplayName("Named parameter exception has clear message")
    void testNamedParamExceptionMessage() throws Exception {
      DatabricksConnection connection = createConnection();
      DatabricksCallableStatement stmt = new DatabricksCallableStatement(connection, CALL_SQL);

      DatabricksSQLFeatureNotSupportedException ex =
          assertThrows(
              DatabricksSQLFeatureNotSupportedException.class,
              () -> stmt.setString("param1", "value"));
      assertTrue(ex.getMessage().contains("Named parameters are not supported"));
      stmt.close();
    }

    @Test
    @DisplayName("Return value syntax exception has clear message")
    void testReturnValueExceptionMessage() throws Exception {
      DatabricksConnection connection = createConnection();

      DatabricksSQLFeatureNotSupportedException ex =
          assertThrows(
              DatabricksSQLFeatureNotSupportedException.class,
              () -> connection.prepareCall("{? = call my_func()}"));
      assertTrue(ex.getMessage().contains("{? = call ...}"));
      assertTrue(ex.getMessage().contains("not supported"));
    }
  }
}
