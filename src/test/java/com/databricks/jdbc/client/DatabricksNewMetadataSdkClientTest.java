package com.databricks.jdbc.client;

import static com.databricks.jdbc.client.impl.sdk.helper.CommandConstants.*;
import static com.databricks.jdbc.client.impl.sdk.helper.MetadataResultConstants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.impl.sdk.DatabricksNewMetadataSdkClient;
import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.client.impl.sdk.helper.ResultColumn;
import com.databricks.jdbc.core.*;
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
  private static final String TEST_SCHEMA = "testSchema";
  private static final String TEST_TABLE = "testTable";
  private static final String TEST_COLUMN = "testColumn";
  private static final String TEST_CATALOG = "catalog1";

  private static Stream<Arguments> listTableTestParams() {
    return Stream.of(
        Arguments.of(
            "SHOW TABLES IN CATALOG `catalog1` SCHEMA LIKE `testSchema` LIKE `testTable`",
            TEST_CATALOG,
            TEST_SCHEMA,
            TEST_TABLE,
            "test for table and schema"),
        Arguments.of(
            "SHOW TABLES IN CATALOG `catalog1`",
            TEST_CATALOG,
            null,
            null,
            "test for all tables and schemas"),
        Arguments.of(
            "SHOW TABLES IN CATALOG `catalog1` SCHEMA LIKE `testSchema`",
            TEST_CATALOG,
            TEST_SCHEMA,
            null,
            "test for all tables"),
        Arguments.of(
            "SHOW TABLES IN CATALOG `catalog1` LIKE `testTable`",
            TEST_CATALOG,
            null,
            TEST_TABLE,
            "test for all schemas"));
  }

  private static Stream<Arguments> listSchemasTestParams() {
    return Stream.of(
        Arguments.of(
            "SHOW SCHEMAS IN `catalog1` LIKE `testSchema`", TEST_SCHEMA, "test for schema"),
        Arguments.of("SHOW SCHEMAS IN `catalog1`", null, "test for all schemas"));
  }

  private static Stream<Arguments> listColumnTestParams() {
    return Stream.of(
        Arguments.of(
            "SHOW COLUMNS IN CATALOG `catalog1` SCHEMA LIKE `testSchema` TABLE LIKE `testTable`",
            TEST_CATALOG,
            TEST_TABLE,
            TEST_SCHEMA,
            null,
            "test for table and schema"),
        Arguments.of(
            "SHOW COLUMNS IN CATALOG `catalog1`",
            TEST_CATALOG,
            null,
            null,
            null,
            "test for all tables and schemas"),
        Arguments.of(
            "SHOW COLUMNS IN CATALOG `catalog1` SCHEMA LIKE `testSchema`",
            TEST_CATALOG,
            null,
            TEST_SCHEMA,
            null,
            "test for schema"),
        Arguments.of(
            "SHOW COLUMNS IN CATALOG `catalog1` TABLE LIKE `testTable`",
            TEST_CATALOG,
            TEST_TABLE,
            null,
            null,
            "test for table"),
        Arguments.of(
            "SHOW COLUMNS IN CATALOG `catalog1` SCHEMA LIKE `testSchema` TABLE LIKE `testTable` LIKE `testColumn`",
            TEST_CATALOG,
            TEST_TABLE,
            TEST_SCHEMA,
            TEST_COLUMN,
            "test for table, schema and column"),
        Arguments.of(
            "SHOW COLUMNS IN CATALOG `catalog1` LIKE `testColumn`",
            TEST_CATALOG,
            null,
            null,
            TEST_COLUMN,
            "test for column"),
        Arguments.of(
            "SHOW COLUMNS IN CATALOG `catalog1` SCHEMA LIKE `testSchema` LIKE `testColumn`",
            TEST_CATALOG,
            null,
            TEST_SCHEMA,
            TEST_COLUMN,
            "test for schema and column"),
        Arguments.of(
            "SHOW COLUMNS IN CATALOG `catalog1` TABLE LIKE `testTable` LIKE `testColumn`",
            TEST_CATALOG,
            TEST_TABLE,
            null,
            TEST_COLUMN,
            "test for table and column"));
  }

  void setupCatalogMocks() throws SQLException {
    when(session.getWarehouseId()).thenReturn(WAREHOUSE_ID);
    when(mockClient.executeStatement(
            "SHOW CATALOGS",
            WAREHOUSE_ID,
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
    when(session.getWarehouseId()).thenReturn(WAREHOUSE_ID);
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
    for (ResultColumn resultColumn : TABLE_COLUMNS) {
      when(mockedResultSet.getObject(resultColumn.getResultSetColumnName()))
          .thenReturn(TEST_COLUMN);
    }
    DatabricksResultSet actualResult = metadataClient.listTables(session, catalog, schema, table);

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
    when(session.getWarehouseId()).thenReturn(WAREHOUSE_ID);
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
    for (ResultColumn resultColumn : COLUMN_COLUMNS) {
      when(mockedResultSet.getObject(resultColumn.getResultSetColumnName()))
          .thenReturn(TEST_COLUMN);
    }
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
    when(session.getWarehouseId()).thenReturn(WAREHOUSE_ID);
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
    for (ResultColumn resultColumn : SCHEMA_COLUMNS) {
      when(mockedResultSet.getObject(resultColumn.getResultSetColumnName()))
          .thenReturn(TEST_COLUMN);
    }
    DatabricksResultSet actualResult = metadataClient.listSchemas(session, TEST_CATALOG, schema);
    assertEquals(
        actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED, description);
    assertEquals(actualResult.statementId(), METADATA_STATEMENT_ID, description);
    assertEquals(
        ((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 1, description);
  }

  @Test
  void testThrowsErrorResultInCaseOfAllCatalogRegex() {
    DatabricksNewMetadataSdkClient metadataClient = new DatabricksNewMetadataSdkClient(mockClient);
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> metadataClient.listColumns(session, "*", TEST_SCHEMA, TEST_TABLE, TEST_COLUMN));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> metadataClient.listTables(session, "*", TEST_SCHEMA, TEST_TABLE));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> metadataClient.listSchemas(session, "*", TEST_SCHEMA));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> metadataClient.listPrimaryKeys(session, "*", TEST_SCHEMA, TEST_TABLE));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> metadataClient.listFunctions(session, "*", TEST_SCHEMA, TEST_TABLE));
  }
}
