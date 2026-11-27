package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.TestConstants.WAREHOUSE_JDBC_URL;
import static com.databricks.jdbc.common.MetadataResultConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionInternal;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.CommandName;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksMetadataQueryClient;
import com.databricks.jdbc.model.core.ResultColumn;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

class MetadataSearchPatternTest {
  private static final String CATALOG = "peco";
  private IDatabricksSession session;
  private IDatabricksMetadataClient client;
  private DatabricksDatabaseMetaData metadata;
  private MetadataResultSetBuilder builder;

  @BeforeEach
  void setUp() throws SQLException {
    IDatabricksConnectionInternal connection = mock(IDatabricksConnectionInternal.class);
    session = mock(IDatabricksSession.class);
    client = mock(IDatabricksMetadataClient.class);
    when(connection.getSession()).thenReturn(session);
    when(session.getDatabricksMetadataClient()).thenReturn(client);
    when(session.isOpen()).thenReturn(true);
    when(session.getConnectionContext())
        .thenReturn(DatabricksConnectionContext.parse(WAREHOUSE_JDBC_URL, new Properties()));
    metadata = new DatabricksDatabaseMetaData(connection);
    builder = new MetadataResultSetBuilder(session.getConnectionContext());
  }

  static Stream<Arguments> escapedPatterns() {
    return Stream.of(
            new String[] {"a\\b", "ab", "ab"},
            new String[] {"a\\\\b", "a_b", "a\\b"},
            new String[] {"a\\\\\\\\b", "a__b", "a\\\\b"},
            new String[] {"a\\\\b%", "a_b%", "a\\b"},
            new String[] {"a\\_b", "a\\_b", "a_b"},
            new String[] {"a\\%b", "a\\%b", "a%b"},
            new String[] {"q'uote", "q_uote", "q'uote"},
            new String[] {"x'q_ji_1598", "x_q_ji_1598", "x'q_ji_1598"},
            new String[] {"x'q\\_ji\\_1598", "x_q\\_ji\\_1598", "x'q_ji_1598"},
            new String[] {"t😀\\_a", "t😀\\_a", "t😀_a"},
            new String[] {"𠮷野家\\_sales", "𠮷野家\\_sales", "𠮷野家_sales"},
            new String[] {"t\\😀\\_a", "t😀\\_a", "t😀_a"},
            new String[] {"t\\𠮷\\_a", "t𠮷\\_a", "t𠮷_a"})
        .flatMap(
            example ->
                Stream.of(
                        CommandName.LIST_SCHEMAS,
                        CommandName.LIST_TABLES,
                        CommandName.LIST_COLUMNS,
                        CommandName.LIST_FUNCTIONS)
                    .map(command -> Arguments.of(command, example[0], example[1], example[2])));
  }

  @ParameterizedTest
  @MethodSource("escapedPatterns")
  void honorsJdbcEscapesAndFiltersWildcardCandidates(
      CommandName command, String pattern, String serverPattern, String expected)
      throws SQLException {
    List<ResultColumn> columns = columns(command);
    List<List<Object>> rows = new ArrayList<>();
    for (String name :
        Arrays.asList(
            "ab",
            "a\\b",
            "a\\\\b",
            "axb",
            "a_b",
            "a%b",
            "q'uote",
            "qxuote",
            "x'q_ji_1598",
            "xxq_ji_1598",
            "t😀_a",
            "t😀xa",
            "𠮷野家_sales",
            "𠮷野家xsales",
            "t𠮷_a",
            "t𠮷xa")) {
      rows.add(row(columns, name));
    }
    DatabricksResultSet candidates =
        builder.getResultSetWithGivenRowsAndColumns(
            columns, rows, "metadata-pattern-test", command);
    stub(command, candidates);
    try (ResultSet result = invoke(command, pattern)) {
      java.sql.ResultSetMetaData before = candidates.getMetaData();
      java.sql.ResultSetMetaData after = result.getMetaData();
      assertEquals(before.getColumnCount(), after.getColumnCount());
      for (int column = 1; column <= columns.size(); column++) {
        assertEquals(before.getColumnName(column), after.getColumnName(column));
        assertEquals(before.getColumnType(column), after.getColumnType(column));
        assertEquals(before.getPrecision(column), after.getPrecision(column));
        assertEquals(before.getScale(column), after.getScale(column));
        assertEquals(before.isNullable(column), after.isNullable(column));
      }
      assertTrue(result.next());
      assertEquals(expected, result.getString(resultName(command)));
      assertFalse(result.next(), "Wildcard candidates must not leak into exact JDBC matches");
    }
    verifyPattern(command, serverPattern);
    assertTrue(candidates.isClosed());
  }

  @ParameterizedTest
  @EnumSource(
      value = CommandName.class,
      names = {"LIST_SCHEMAS", "LIST_TABLES", "LIST_COLUMNS"})
  void closesCandidatesWhenFilteringFails(CommandName command) throws SQLException {
    DatabricksResultSet candidates =
        spy(
            builder.getResultSetWithGivenRowsAndColumns(
                columns(command),
                Arrays.asList(row(columns(command), "a\\b")),
                "metadata-pattern-test",
                command));
    doThrow(new SQLException("metadata read failed")).when(candidates).getString(anyInt());
    stub(command, candidates);
    assertThrows(SQLException.class, () -> invoke(command, "a\\\\b"));
    assertTrue(candidates.isClosed());
  }

  @ParameterizedTest
  @EnumSource(
      value = CommandName.class,
      names = {"LIST_SCHEMAS", "LIST_TABLES", "LIST_COLUMNS", "LIST_FUNCTIONS"})
  void leavesSimplePatternsAndResultSetsUntouched(CommandName command) throws SQLException {
    for (String pattern : new String[] {"plain", "%", "_", "a%b_"}) {
      DatabricksResultSet result =
          builder.getResultSetWithGivenRowsAndColumns(
              columns(command), new ArrayList<>(), "metadata-pattern-test", command);
      stub(command, result);
      assertSame(result, invoke(command, pattern));
      verifyPattern(command, pattern);
      clearInvocations(client);
      assertFalse(result.isClosed());
      result.close();
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = CommandName.class,
      names = {"LIST_TABLES", "LIST_COLUMNS"})
  void preservesNullSchemaRowsForEscapedAndPlainPatterns(CommandName command) throws SQLException {
    for (String schema : new String[] {"rv_ji_1598", "rv\\_ji\\_1598"}) {
      List<Object> temporary = row(columns(command), "tv_rv_ji_1598");
      temporary.set(1, null);
      List<Object> regular = row(columns(command), "rv_ji_1598");
      stub(
          command,
          builder.getResultSetWithGivenRowsAndColumns(
              columns(command),
              Arrays.asList(temporary, regular),
              "metadata-pattern-test",
              command));
      try (ResultSet result =
          command == CommandName.LIST_TABLES
              ? metadata.getTables(null, schema, "%", null)
              : metadata.getColumns(null, schema, "%", "%")) {
        List<String> schemas = new ArrayList<>();
        while (result.next()) {
          schemas.add(result.getString("TABLE_SCHEM"));
        }
        assertEquals(Arrays.asList(null, "rv_ji_1598"), schemas);
      }
    }
  }

  static Stream<Arguments> sqlPatterns() {
    return Stream.of(
            new String[] {"a\\\\b", "a.b"},
            new String[] {"a\\\\'b", "a..b"},
            new String[] {"a\\_b'", "a_b."},
            new String[] {"a\\%b", "a%b"},
            new String[] {"x\\", "x"},
            new String[] {"t😀\\_a", "t😀_a"},
            new String[] {"o'x_ji_1598", "o.x.ji.1598"})
        .flatMap(
            example ->
                Stream.of(
                        CommandName.LIST_SCHEMAS,
                        CommandName.LIST_TABLES,
                        CommandName.LIST_COLUMNS,
                        CommandName.LIST_FUNCTIONS)
                    .map(command -> Arguments.of(command, example[0], example[1])));
  }

  @ParameterizedTest
  @MethodSource("sqlPatterns")
  void emitsSqlThroughRealMetadataQueryClient(CommandName command, String pattern, String literal)
      throws SQLException {
    IDatabricksClient transport = mock(IDatabricksClient.class);
    com.databricks.jdbc.api.internal.IDatabricksConnectionContext context =
        session.getConnectionContext();
    when(transport.getConnectionContext()).thenReturn(context);
    when(session.getCurrentCatalog()).thenReturn(CATALOG);
    DatabricksMetadataQueryClient queryClient = new DatabricksMetadataQueryClient(transport);
    when(session.getDatabricksMetadataClient()).thenReturn(queryClient);
    when(transport.executeStatement(anyString(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            builder.getResultSetWithGivenRowsAndColumns(
                columns(command), new ArrayList<>(), "metadata-sql-test", command));
    try (ResultSet result = invoke(command, pattern)) {
      assertFalse(result.next());
    }
    String like = " LIKE '" + literal + "'";
    String expected;
    switch (command) {
      case LIST_SCHEMAS:
        expected = "SHOW SCHEMAS IN \u0060peco\u0060" + like;
        break;
      case LIST_TABLES:
        expected = "SHOW TABLES IN CATALOG \u0060peco\u0060 SCHEMA" + like + like;
        break;
      case LIST_COLUMNS:
        expected =
            "SHOW COLUMNS IN CATALOG \u0060peco\u0060 SCHEMA" + like + " TABLE" + like + like;
        break;
      case LIST_FUNCTIONS:
        expected = "SHOW FUNCTIONS IN CATALOG \u0060peco\u0060 SCHEMA" + like + like;
        break;
      default:
        throw new AssertionError(command);
    }
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    verify(transport)
        .executeStatement(sql.capture(), any(), any(), any(), eq(session), any(), any());
    assertEquals(expected, sql.getValue());
  }

  private List<ResultColumn> columns(CommandName command) {
    switch (command) {
      case LIST_SCHEMAS:
        return SCHEMA_COLUMNS;
      case LIST_TABLES:
        return TABLE_COLUMNS;
      case LIST_COLUMNS:
        return COLUMN_COLUMNS;
      case LIST_FUNCTIONS:
        return FUNCTION_COLUMNS;
      default:
        throw new AssertionError(command);
    }
  }

  private List<Object> row(List<ResultColumn> columns, String name) {
    List<Object> values = new ArrayList<>();
    for (ResultColumn column : columns) {
      String label = column.getColumnName();
      values.add(
          label.equals("TABLE_SCHEM")
                  || label.equals("TABLE_NAME")
                  || label.equals("COLUMN_NAME")
                  || label.equals("FUNCTION_SCHEM")
                  || label.equals("FUNCTION_NAME")
              ? name
              : null);
    }
    return values;
  }

  private String resultName(CommandName command) {
    switch (command) {
      case LIST_SCHEMAS:
        return "TABLE_SCHEM";
      case LIST_TABLES:
        return "TABLE_NAME";
      case LIST_COLUMNS:
        return "COLUMN_NAME";
      case LIST_FUNCTIONS:
        return "FUNCTION_NAME";
      default:
        throw new AssertionError(command);
    }
  }

  private void stub(CommandName command, DatabricksResultSet result) throws SQLException {
    switch (command) {
      case LIST_SCHEMAS:
        when(client.listSchemas(any(), any(), any())).thenReturn(result);
        break;
      case LIST_TABLES:
        when(client.listTables(any(), any(), any(), any(), any())).thenReturn(result);
        break;
      case LIST_COLUMNS:
        when(client.listColumns(any(), any(), any(), any(), any())).thenReturn(result);
        break;
      case LIST_FUNCTIONS:
        when(client.listFunctions(any(), any(), any(), any())).thenReturn(result);
        break;
      default:
        throw new AssertionError(command);
    }
  }

  private ResultSet invoke(CommandName command, String pattern) throws SQLException {
    switch (command) {
      case LIST_SCHEMAS:
        return metadata.getSchemas(CATALOG, pattern);
      case LIST_TABLES:
        return metadata.getTables(CATALOG, pattern, pattern, null);
      case LIST_COLUMNS:
        return metadata.getColumns(CATALOG, pattern, pattern, pattern);
      case LIST_FUNCTIONS:
        return metadata.getFunctions(CATALOG, pattern, pattern);
      default:
        throw new AssertionError(command);
    }
  }

  private void verifyPattern(CommandName command, String pattern) throws SQLException {
    switch (command) {
      case LIST_SCHEMAS:
        verify(client).listSchemas(session, CATALOG, pattern);
        break;
      case LIST_TABLES:
        verify(client).listTables(session, CATALOG, pattern, pattern, null);
        break;
      case LIST_COLUMNS:
        verify(client).listColumns(session, CATALOG, pattern, pattern, pattern);
        break;
      case LIST_FUNCTIONS:
        verify(client).listFunctions(session, CATALOG, pattern, pattern);
        break;
      default:
        throw new AssertionError(command);
    }
  }
}
