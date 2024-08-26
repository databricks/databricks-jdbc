package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.common.MetadataResultConstants.*;
import static com.databricks.jdbc.dbclient.impl.common.CommandConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.impl.DatabricksResultSetMetaData;
import com.databricks.jdbc.api.impl.ImmutableSqlParameter;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.core.ResultColumn;
import com.databricks.sdk.service.sql.StatementState;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksNewMetadataSdkClientTest {
  @Mock private static DatabricksSdkClient mockClient;
  @Mock private static DatabricksResultSet mockedCatalogResultSet;
  @Mock private static DatabricksResultSet mockedResultSet;
  @Mock private static IDatabricksSession session;
  @Mock private static IDatabricksComputeResource mockedComputeResource;

  private static Stream<Arguments> listTableTestParams() {
    return Stream.of(
        Arguments.of(
            "SHOW TABLES IN CATALOG catalog1 SCHEMA LIKE 'testSchema' LIKE 'testTable'",
            TEST_CATALOG,
            TEST_SCHEMA,
            TEST_TABLE,
            "test for table and schema"),
        Arguments.of(
            "SHOW TABLES IN CATALOG catalog1",
            TEST_CATALOG,
            null,
            null,
            "test for all tables and schemas"),
        Arguments.of(
            "SHOW TABLES IN CATALOG catalog1 SCHEMA LIKE 'testSchema'",
            TEST_CATALOG,
            TEST_SCHEMA,
            null,
            "test for all tables"),
        Arguments.of(
            "SHOW TABLES IN CATALOG catalog1 LIKE 'testTable'",
            TEST_CATALOG,
            null,
            TEST_TABLE,
            "test for all schemas"));
  }

  private static Stream<Arguments> listSchemasTestParams() {
    return Stream.of(
        Arguments.of("SHOW SCHEMAS IN catalog1 LIKE 'testSchema'", TEST_SCHEMA, "test for schema"),
        Arguments.of("SHOW SCHEMAS IN catalog1", null, "test for all schemas"));
  }

  private static Stream<Arguments> listFunctionsTestParams() {
    return Stream.of(
        Arguments.of(
            "SHOW FUNCTIONS IN CATALOG catalog1 SCHEMA LIKE 'testSchema' LIKE 'functionPattern'",
            TEST_CATALOG,
            TEST_SCHEMA,
            TEST_FUNCTION_PATTERN,
            "test for get functions"),
        Arguments.of(
            "SHOW FUNCTIONS IN CATALOG catalog1 LIKE 'functionPattern'",
            TEST_CATALOG,
            null,
            TEST_FUNCTION_PATTERN,
            "test for get functions without schema"),
        Arguments.of(
            "SHOW FUNCTIONS IN CATALOG catalog1 SCHEMA LIKE 'testSchema'",
            TEST_CATALOG,
            TEST_SCHEMA,
            null,
            "test for get functions without function pattern"));
  }

  private static Stream<Arguments> listColumnTestParams() {
    return Stream.of(
        Arguments.of(
            "SHOW COLUMNS IN CATALOG catalog1 SCHEMA LIKE 'testSchema' TABLE LIKE 'testTable'",
            TEST_CATALOG,
            TEST_TABLE,
            TEST_SCHEMA,
            null,
            "test for table and schema"),
        Arguments.of(
            "SHOW COLUMNS IN CATALOG catalog1",
            TEST_CATALOG,
            null,
            null,
            null,
            "test for all tables and schemas"),
        Arguments.of(
            "SHOW COLUMNS IN CATALOG catalog1 SCHEMA LIKE 'testSchema'",
            TEST_CATALOG,
            null,
            TEST_SCHEMA,
            null,
            "test for schema"),
        Arguments.of(
            "SHOW COLUMNS IN CATALOG catalog1 TABLE LIKE 'testTable'",
            TEST_CATALOG,
            TEST_TABLE,
            null,
            null,
            "test for table"),
        Arguments.of(
            "SHOW COLUMNS IN CATALOG catalog1 SCHEMA LIKE 'testSchema' TABLE LIKE 'testTable' LIKE 'testColumn'",
            TEST_CATALOG,
            TEST_TABLE,
            TEST_SCHEMA,
            TEST_COLUMN,
            "test for table, schema and column"),
        Arguments.of(
            "SHOW COLUMNS IN CATALOG catalog1 LIKE 'testColumn'",
            TEST_CATALOG,
            null,
            null,
            TEST_COLUMN,
            "test for column"),
        Arguments.of(
            "SHOW COLUMNS IN CATALOG catalog1 SCHEMA LIKE 'testSchema' LIKE 'testColumn'",
            TEST_CATALOG,
            null,
            TEST_SCHEMA,
            TEST_COLUMN,
            "test for schema and column"),
        Arguments.of(
            "SHOW COLUMNS IN CATALOG catalog1 TABLE LIKE 'testTable' LIKE 'testColumn'",
            TEST_CATALOG,
            TEST_TABLE,
            null,
            TEST_COLUMN,
            "test for table and column"));
  }

  void setupCatalogMocks() throws SQLException {
    when(session.getComputeResource()).thenReturn(mockedComputeResource);
    when(mockClient.executeStatement(
            "SHOW CATALOGS",
            mockedComputeResource,
            new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.METADATA,
            session,
            null))
        .thenReturn(mockedCatalogResultSet);
    when(mockedCatalogResultSet.next()).thenReturn(true, true, false);
    for (ResultColumn resultColumn : CATALOG_COLUMNS) {
      when(mockedCatalogResultSet.getObject(resultColumn.getResultSetColumnName()))
          .thenReturn(TEST_COLUMN);
    }
  }

  @Test
  void testListCatalogs() throws SQLException {
    setupCatalogMocks();
    DatabricksNewMetadataSdkClient metadataClient = new DatabricksNewMetadataSdkClient(mockClient);
    DatabricksResultSet actualResult = metadataClient.listCatalogs(session);
    assertEquals(actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED);
    assertEquals(actualResult.statementId(), GET_CATALOGS_STATEMENT_ID);
    assertEquals(((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 2);
  }

  @Test
  void testListTableTypes() throws SQLException {
    DatabricksNewMetadataSdkClient metadataClient = new DatabricksNewMetadataSdkClient(mockClient);
    DatabricksResultSet actualResult = metadataClient.listTableTypes(session);
    assertEquals(actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED);
    assertEquals(actualResult.statementId(), GET_TABLE_TYPE_STATEMENT_ID);
    assertEquals(((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 3);
  }

  @ParameterizedTest
  @MethodSource("listTableTestParams")
  void testListTables(
      String sqlStatement, String catalog, String schema, String table, String description)
      throws SQLException {
    when(session.getComputeResource()).thenReturn(mockedComputeResource);
    DatabricksNewMetadataSdkClient metadataClient = new DatabricksNewMetadataSdkClient(mockClient);
    when(mockClient.executeStatement(
            sqlStatement,
            mockedComputeResource,
            new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.METADATA,
            session,
            null))
        .thenReturn(mockedResultSet);
    when(mockedResultSet.next()).thenReturn(true, false);
    for (ResultColumn resultColumn : TABLE_COLUMNS) {
      if (resultColumn == TABLE_COLUMNS.get(3)) {
        when(mockedResultSet.getObject(resultColumn.getResultSetColumnName())).thenReturn("TABLE");
      } else
        when(mockedResultSet.getObject(resultColumn.getResultSetColumnName()))
            .thenReturn(TEST_COLUMN);
    }
    DatabricksResultSet actualResult =
        metadataClient.listTables(session, catalog, schema, table, null);

    assertEquals(
        actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED, description);
    assertEquals(actualResult.statementId(), GET_TABLES_STATEMENT_ID, description);
    assertEquals(
        ((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 1, description);
  }

  @ParameterizedTest
  @MethodSource("listColumnTestParams")
  void testListColumns(
      String sqlStatement,
      String catalog,
      String table,
      String schema,
      String column,
      String description)
      throws SQLException {
    when(session.getComputeResource()).thenReturn(mockedComputeResource);
    DatabricksNewMetadataSdkClient metadataClient = new DatabricksNewMetadataSdkClient(mockClient);
    when(mockClient.executeStatement(
            sqlStatement,
            mockedComputeResource,
            new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.QUERY,
            session,
            null))
        .thenReturn(mockedResultSet);
    when(mockedResultSet.next()).thenReturn(true, false);
    DatabricksResultSet actualResult =
        metadataClient.listColumns(session, catalog, schema, table, column);

    assertEquals(
        actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED, description);
    assertEquals(actualResult.statementId(), METADATA_STATEMENT_ID, description);
    assertEquals(
        ((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 1, description);
  }

  @ParameterizedTest
  @MethodSource("listSchemasTestParams")
  void testListSchemas(String sqlStatement, String schema, String description) throws SQLException {
    when(session.getComputeResource()).thenReturn(mockedComputeResource);
    DatabricksNewMetadataSdkClient metadataClient = new DatabricksNewMetadataSdkClient(mockClient);
    when(mockClient.executeStatement(
            sqlStatement,
            mockedComputeResource,
            new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.METADATA,
            session,
            null))
        .thenReturn(mockedResultSet);
    when(mockedResultSet.next()).thenReturn(true, false);
    when(mockedResultSet.getObject("databaseName")).thenReturn(TEST_COLUMN);
    DatabricksResultSet actualResult = metadataClient.listSchemas(session, TEST_CATALOG, schema);
    assertEquals(
        actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED, description);
    assertEquals(actualResult.statementId(), METADATA_STATEMENT_ID, description);
    assertEquals(
        ((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 1, description);
  }

  @Test
  void testListPrimaryKeys() throws SQLException {
    when(session.getComputeResource()).thenReturn(WAREHOUSE_COMPUTE);
    DatabricksNewMetadataSdkClient metadataClient = new DatabricksNewMetadataSdkClient(mockClient);
    when(mockClient.executeStatement(
            "SHOW KEYS IN CATALOG catalog1 IN SCHEMA testSchema IN TABLE testTable",
            WAREHOUSE_COMPUTE,
            new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.METADATA,
            session,
            null))
        .thenReturn(mockedResultSet);
    when(mockedResultSet.next()).thenReturn(true, false);
    for (ResultColumn resultColumn : PRIMARY_KEYS_COLUMNS) {
      when(mockedResultSet.getObject(resultColumn.getResultSetColumnName()))
          .thenReturn(TEST_COLUMN);
    }
    DatabricksResultSet actualResult =
        metadataClient.listPrimaryKeys(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
    assertEquals(actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED);
    assertEquals(actualResult.statementId(), METADATA_STATEMENT_ID);
    assertEquals(((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 1);
  }

  @ParameterizedTest
  @MethodSource("listFunctionsTestParams")
  void testTestFunctions(
      String sql, String catalog, String schema, String functionPattern, String description)
      throws SQLException {
    when(session.getComputeResource()).thenReturn(WAREHOUSE_COMPUTE);
    DatabricksNewMetadataSdkClient metadataClient = new DatabricksNewMetadataSdkClient(mockClient);
    when(mockClient.executeStatement(
            sql,
            WAREHOUSE_COMPUTE,
            new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.QUERY,
            session,
            null))
        .thenReturn(mockedResultSet);
    when(mockedResultSet.next()).thenReturn(true, false);
    DatabricksResultSet actualResult =
        metadataClient.listFunctions(session, catalog, schema, functionPattern);

    assertEquals(
        actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED, description);
    assertEquals(actualResult.statementId(), GET_FUNCTIONS_STATEMENT_ID, description);
    assertEquals(
        ((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 1, description);
  }

  @Test
  void testThrowsErrorResultInCaseOfNullCatalog() {
    DatabricksNewMetadataSdkClient metadataClient = new DatabricksNewMetadataSdkClient(mockClient);
    assertThrows(
        DatabricksValidationException.class,
        () -> metadataClient.listColumns(session, null, TEST_SCHEMA, TEST_TABLE, TEST_COLUMN));
    assertThrows(
        DatabricksValidationException.class,
        () -> metadataClient.listTables(session, null, TEST_SCHEMA, TEST_TABLE, null));
    assertThrows(
        DatabricksValidationException.class,
        () -> metadataClient.listSchemas(session, null, TEST_SCHEMA));
    assertThrows(
        DatabricksValidationException.class,
        () -> metadataClient.listPrimaryKeys(session, null, TEST_SCHEMA, TEST_TABLE));
    assertThrows(
        DatabricksValidationException.class,
        () -> metadataClient.listFunctions(session, null, TEST_SCHEMA, TEST_TABLE));
  }

  @Test
  void testListTypeInfo() {
    DatabricksNewMetadataSdkClient metadataClient = new DatabricksNewMetadataSdkClient(mockClient);
    assertNotNull(metadataClient.listTypeInfo(session));
  }
}
