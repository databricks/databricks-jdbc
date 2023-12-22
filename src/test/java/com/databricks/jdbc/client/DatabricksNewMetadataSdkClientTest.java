package com.databricks.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.impl.sdk.DatabricksNewMetadataSdkClient;
import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.DatabricksResultSetMetaData;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.core.ImmutableSqlParameter;
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
  private static final String WAREHOUSE_ID = "warehouse_id";

  private static Stream<Arguments> listTableTestParams() {
    return Stream.of(
        Arguments.of(
            "show jdbc_tables in `catalog1` like `testTable` schema like `testSchema`",
            "*",
            "testSchema",
            "testTable",
            "test for table and schema"),
        Arguments.of(
            "show jdbc_tables in `catalog1`", "*", "*", "*", "test for all tables and schemas"),
        Arguments.of(
            "show jdbc_tables in `catalog1` schema like `testSchema`",
            "*",
            "testSchema",
            "*",
            "test for all tables"),
        Arguments.of(
            "show jdbc_tables in `catalog1` like `testTable`",
            "*",
            "*",
            "testTable",
            "test for all schemas"));
  }

  private static Stream<Arguments> listSchemasTestParams() {
    return Stream.of(
        Arguments.of(
            "show schemas in `catalog1` like `testSchema`", "testSchema", "test for schema"),
        Arguments.of("show schemas in `catalog1`", "*", "test for all schemas"));
  }

  private static Stream<Arguments> listColumnTestParams() {
    return Stream.of(
        Arguments.of(
            "show jdbc_columns in `catalog1` table like `testTable` schema like `testSchema`",
            "*",
            "testTable",
            "testSchema",
            "*",
            "test for table and schema"),
        Arguments.of(
            "show jdbc_columns in `catalog1`",
            "*",
            "*",
            "*",
            "*",
            "test for all tables and schemas"),
        Arguments.of(
            "show jdbc_columns in `catalog1` schema like `testSchema`",
            "*",
            "*",
            "testSchema",
            "*",
            "test for schema"),
        Arguments.of(
            "show jdbc_columns in `catalog1` table like `testTable`",
            "*",
            "testTable",
            "*",
            "*",
            "test for table"),
        Arguments.of(
            "show jdbc_columns in `catalog1` like `testColumn` table like `testTable` schema like `testSchema`",
            "*",
            "testTable",
            "testSchema",
            "testColumn",
            "test for table, schema and column"),
        Arguments.of(
            "show jdbc_columns in `catalog1` like `testColumn`",
            "*",
            "*",
            "*",
            "testColumn",
            "test for column"),
        Arguments.of(
            "show jdbc_columns in `catalog1` like `testColumn` schema like `testSchema`",
            "*",
            "*",
            "testSchema",
            "testColumn",
            "test for schema and column"),
        Arguments.of(
            "show jdbc_columns in `catalog1` like `testColumn` table like `testTable`",
            "*",
            "testTable",
            "*",
            "testColumn",
            "test for table and column"));
  }

  void setupCatalogMocks() throws SQLException {
    when(session.getWarehouseId()).thenReturn(WAREHOUSE_ID);
    when(mockClient.executeStatement(
            "show catalogs",
            WAREHOUSE_ID,
            new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.METADATA,
            session,
            null))
        .thenReturn(mockedCatalogResultSet);
    when(mockedCatalogResultSet.next()).thenReturn(true, true, false);
    when(mockedCatalogResultSet.getString(1)).thenReturn("catalog1", "catalog2");
  }

  void setupCatalogMockSingleResponse() throws SQLException {
    when(session.getWarehouseId()).thenReturn(WAREHOUSE_ID);
    when(mockClient.executeStatement(
            "show catalogs",
            WAREHOUSE_ID,
            new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.METADATA,
            session,
            null))
        .thenReturn(mockedCatalogResultSet);
    when(mockedCatalogResultSet.next()).thenReturn(true, false);
    when(mockedCatalogResultSet.getString(1)).thenReturn("catalog1");
  }

  @Test
  void testListCatalogs() throws SQLException {
    setupCatalogMocks();
    DatabricksNewMetadataSdkClient metadataClient = new DatabricksNewMetadataSdkClient(mockClient);
    DatabricksResultSet actualResult = metadataClient.listCatalogs(session);

    assertEquals(actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED);
    assertEquals(actualResult.statementId(), "getcatalogs-metadata");
    assertEquals(((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 2);
  }

  @ParameterizedTest
  @MethodSource("listTableTestParams")
  void testListTables(
      String sqlStatement, String catalog, String schema, String table, String description)
      throws SQLException {
    setupCatalogMockSingleResponse();
    DatabricksNewMetadataSdkClient metadataClient = new DatabricksNewMetadataSdkClient(mockClient);
    when(mockClient.executeStatement(
            sqlStatement,
            WAREHOUSE_ID,
            new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.METADATA,
            session,
            null))
        .thenReturn(mockedResultSet);
    when(mockedResultSet.next()).thenReturn(true, false);
    when(mockedResultSet.getString(2)).thenReturn("table1");
    DatabricksResultSet actualResult = metadataClient.listTables(session, catalog, schema, table);

    assertEquals(
        actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED, description);
    assertEquals(actualResult.statementId(), "gettables-metadata", description);
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
    setupCatalogMockSingleResponse();
    DatabricksNewMetadataSdkClient metadataClient = new DatabricksNewMetadataSdkClient(mockClient);
    when(mockClient.executeStatement(
            sqlStatement,
            WAREHOUSE_ID,
            new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.METADATA,
            session,
            null))
        .thenReturn(mockedResultSet);
    when(mockedResultSet.next()).thenReturn(true, false);
    when(mockedResultSet.getString(2)).thenReturn("column1");
    DatabricksResultSet actualResult =
        metadataClient.listColumns(session, catalog, schema, table, column);

    assertEquals(
        actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED, description);
    assertEquals(actualResult.statementId(), "metadata-statement", description);
    assertEquals(
        ((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 1, description);
  }

  @ParameterizedTest
  @MethodSource("listSchemasTestParams")
  void testListTables(String sqlStatement, String schema, String description) throws SQLException {
    setupCatalogMockSingleResponse();
    DatabricksNewMetadataSdkClient metadataClient = new DatabricksNewMetadataSdkClient(mockClient);
    when(mockClient.executeStatement(
            sqlStatement,
            WAREHOUSE_ID,
            new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.METADATA,
            session,
            null))
        .thenReturn(mockedResultSet);
    when(mockedResultSet.next()).thenReturn(true, false);
    when(mockedResultSet.getString(2)).thenReturn("schema1");
    DatabricksResultSet actualResult = metadataClient.listSchemas(session, "*", schema);

    assertEquals(
        actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED, description);
    assertEquals(actualResult.statementId(), "metadata-statement", description);
    assertEquals(
        ((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 1, description);
  }
}
