package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.common.MetadataResultConstants.*;
import static com.databricks.jdbc.dbclient.impl.common.CommandConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.impl.DatabricksResultSetMetaData;
import com.databricks.jdbc.api.impl.ImmutableSqlParameter;
import com.databricks.jdbc.common.CommandName;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.core.ResultColumn;
import com.databricks.sdk.service.sql.StatementState;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksMetadataSdkClientTest {
  @Mock private static DatabricksSdkClient mockClient;
  @Mock private static DatabricksResultSet mockedCatalogResultSet;
  @Mock private static DatabricksResultSet mockedResultSet;
  @Mock private static IDatabricksSession session;
  @Mock private static IDatabricksComputeResource mockedComputeResource;
  @Mock private static ResultSetMetaData mockedMetaData;

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
    DatabricksMetadataSdkClient metadataClient = new DatabricksMetadataSdkClient(mockClient);
    doReturn(1).when(mockedMetaData).getColumnCount();
    doReturn(CATALOG_COLUMN_FOR_GET_CATALOGS.getResultSetColumnName())
        .when(mockedMetaData)
        .getColumnName(1);
    doReturn(255).when(mockedMetaData).getPrecision(1);
    doReturn(0).when(mockedMetaData).getScale(1);
    when(mockedCatalogResultSet.getMetaData()).thenReturn(mockedMetaData);
    DatabricksResultSet actualResult = metadataClient.listCatalogs(session);
    assertEquals(actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED);
    assertEquals(actualResult.getStatementId(), GET_CATALOGS_STATEMENT_ID);
    assertEquals(((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 2);
  }

  @Test
  void testListTableTypes() throws SQLException {
    DatabricksMetadataSdkClient metadataClient = new DatabricksMetadataSdkClient(mockClient);
    DatabricksResultSet actualResult = metadataClient.listTableTypes(session);
    assertEquals(actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED);
    assertEquals(actualResult.getStatementId(), GET_TABLE_TYPE_STATEMENT_ID);
    assertEquals(((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 3);
  }

  @ParameterizedTest
  @MethodSource("listTableTestParams")
  void testListTables(
      String sqlStatement, String catalog, String schema, String table, String description)
      throws SQLException {

    when(session.getComputeResource()).thenReturn(mockedComputeResource);
    DatabricksMetadataSdkClient metadataClient = new DatabricksMetadataSdkClient(mockClient);

    // Mock the metadata to return for each column
    // mockedMetaData represents resultManifest recieved from the server
    doReturn(7).when(mockedMetaData).getColumnCount();

    doReturn(SCHEMA_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(1);
    doReturn(255).when(mockedMetaData).getPrecision(1);
    doReturn(0).when(mockedMetaData).getScale(1);

    doReturn(TABLE_NAME_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(2);
    doReturn(255).when(mockedMetaData).getPrecision(2);
    doReturn(0).when(mockedMetaData).getScale(2);

    doReturn("isTemporary").when(mockedMetaData).getColumnName(3);
    doReturn("information").when(mockedMetaData).getColumnName(4);

    doReturn(CATALOG_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(5);
    doReturn(255).when(mockedMetaData).getPrecision(5);
    doReturn(0).when(mockedMetaData).getScale(5);

    doReturn(TABLE_TYPE_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(6);
    doReturn(255).when(mockedMetaData).getPrecision(6);
    doReturn(0).when(mockedMetaData).getScale(6);

    doReturn(REMARKS_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(7);
    doReturn(255).when(mockedMetaData).getPrecision(7);
    doReturn(0).when(mockedMetaData).getScale(7);

    // Mock the client call
    when(mockClient.executeStatement(
            (sqlStatement),
            (mockedComputeResource),
            new HashMap<Integer, ImmutableSqlParameter>(),
            (StatementType.METADATA),
            (session),
            null))
        .thenReturn(mockedResultSet);

    // Mock result set iteration
    when(mockedResultSet.next()).thenReturn(true, false);
    for (ResultColumn resultColumn : TABLE_COLUMNS) {
      if (resultColumn == TABLE_COLUMNS.get(3)) {
        when(mockedResultSet.getObject(resultColumn.getResultSetColumnName())).thenReturn("TABLE");
      } else {
        when(mockedResultSet.getObject(resultColumn.getResultSetColumnName()))
            .thenReturn(TEST_COLUMN);
      }
    }

    // Set the mocked metadata for the result set
    when(mockedResultSet.getMetaData()).thenReturn(mockedMetaData);

    // Execute the test
    DatabricksResultSet actualResult =
        metadataClient.listTables(session, catalog, schema, table, null);

    // Validate the result set and metadata
    assertEquals(
        actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED, description);
    assertEquals(actualResult.getStatementId(), GET_TABLES_STATEMENT_ID, description);
    assertEquals(
        ((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 1, description);

    // Verify metadata properties
    ResultSetMetaData actualMetaData = actualResult.getMetaData();
    assertEquals(actualMetaData.getColumnCount(), 10);
    assertEquals(actualMetaData.getColumnName(1), "TABLE_CAT");
    assertEquals(actualMetaData.getColumnType(1), Types.VARCHAR);
    assertEquals(actualMetaData.getPrecision(1), 128);
    assertEquals(actualMetaData.isNullable(1), ResultSetMetaData.columnNullable);

    assertEquals(actualMetaData.getColumnName(2), "TABLE_SCHEM");
    assertEquals(actualMetaData.getColumnType(2), Types.VARCHAR);
    assertEquals(actualMetaData.getPrecision(2), 128);
    assertEquals(actualMetaData.isNullable(2), ResultSetMetaData.columnNullable);

    assertEquals(actualMetaData.getColumnName(3), "TABLE_NAME");
    assertEquals(actualMetaData.getColumnType(3), Types.VARCHAR);
    assertEquals(actualMetaData.getPrecision(3), 128);
    assertEquals(actualMetaData.isNullable(3), ResultSetMetaData.columnNoNulls);

    assertEquals(actualMetaData.getColumnName(4), "TABLE_TYPE");
    assertEquals(actualMetaData.getColumnType(4), Types.VARCHAR);
    assertEquals(actualMetaData.getPrecision(4), 128);
    assertEquals(actualMetaData.isNullable(4), ResultSetMetaData.columnNoNulls);

    assertEquals(actualMetaData.getColumnName(5), "REMARKS");
    assertEquals(actualMetaData.getColumnType(5), Types.VARCHAR);
    assertEquals(actualMetaData.getPrecision(5), 254);
    assertEquals(actualMetaData.isNullable(5), ResultSetMetaData.columnNullable);
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
    DatabricksMetadataSdkClient metadataClient = new DatabricksMetadataSdkClient(mockClient);
    when(mockClient.executeStatement(
            sqlStatement,
            mockedComputeResource,
            new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.METADATA,
            session,
            null))
        .thenReturn(mockedResultSet);
    when(mockedResultSet.next()).thenReturn(true, false);

    doReturn(13).when(mockedMetaData).getColumnCount();
    doReturn(COL_NAME_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(1);
    doReturn(CATALOG_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(2);
    doReturn(SCHEMA_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(3);
    doReturn(TABLE_NAME_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(4);
    doReturn(COLUMN_TYPE_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(5);
    doReturn(COLUMN_SIZE_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(6);
    doReturn(DECIMAL_DIGITS_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(7);
    doReturn(NUM_PREC_RADIX_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(8);
    doReturn(NULLABLE_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(9);
    doReturn(REMARKS_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(10);
    doReturn(ORDINAL_POSITION_COLUMN.getResultSetColumnName())
        .when(mockedMetaData)
        .getColumnName(11);
    doReturn(IS_AUTO_INCREMENT_COLUMN.getResultSetColumnName())
        .when(mockedMetaData)
        .getColumnName(12);
    doReturn(IS_GENERATED_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(13);

    when(mockedResultSet.getMetaData()).thenReturn(mockedMetaData);

    DatabricksResultSet actualResult =
        metadataClient.listColumns(session, catalog, schema, table, column);

    assertEquals(
        actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED, description);
    assertEquals(actualResult.getStatementId(), METADATA_STATEMENT_ID, description);
    assertEquals(
        ((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 1, description);

    // verify metadata properties
    ResultSetMetaData actualMetaData = actualResult.getMetaData();
    assertEquals(actualMetaData.getColumnCount(), COLUMN_COLUMNS.size());
    List<ResultColumn> non_nullable_columns =
        NON_NULLABLE_COLUMNS_MAP.get(CommandName.LIST_COLUMNS);
    for (int i = 0; i < COLUMN_COLUMNS.size(); i++) {
      ResultColumn resultColumn = COLUMN_COLUMNS.get(i);
      assertEquals(actualMetaData.getColumnName(i + 1), resultColumn.getColumnName());
      assertEquals(actualMetaData.getColumnType(i + 1), resultColumn.getColumnTypeInt());
      assertEquals(actualMetaData.getColumnTypeName(i + 1), resultColumn.getColumnTypeString());
      if (LARGE_DISPLAY_COLUMNS.contains(resultColumn)) {
        assertEquals(actualMetaData.getPrecision(i + 1), 254);
      } else {
        assertEquals(actualMetaData.getPrecision(i + 1), resultColumn.getColumnPrecision());
      }

      if (non_nullable_columns.contains(resultColumn)) {
        assertEquals(actualMetaData.isNullable(i + 1), ResultSetMetaData.columnNoNulls);
      } else {
        assertEquals(actualMetaData.isNullable(i + 1), ResultSetMetaData.columnNullable);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("listSchemasTestParams")
  void testListSchemas(String sqlStatement, String schema, String description) throws SQLException {
    when(session.getComputeResource()).thenReturn(mockedComputeResource);
    DatabricksMetadataSdkClient metadataClient = new DatabricksMetadataSdkClient(mockClient);
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
    doReturn(2).when(mockedMetaData).getColumnCount();
    doReturn(SCHEMA_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(1);
    doReturn(CATALOG_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(2);
    when(mockedResultSet.getMetaData()).thenReturn(mockedMetaData);
    DatabricksResultSet actualResult = metadataClient.listSchemas(session, TEST_CATALOG, schema);
    assertEquals(
        actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED, description);
    assertEquals(actualResult.getStatementId(), METADATA_STATEMENT_ID, description);
    assertEquals(
        ((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 1, description);
  }

  @Test
  void testListPrimaryKeys() throws SQLException {
    when(session.getComputeResource()).thenReturn(WAREHOUSE_COMPUTE);
    DatabricksMetadataSdkClient metadataClient = new DatabricksMetadataSdkClient(mockClient);
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
    doReturn(6).when(mockedMetaData).getColumnCount();
    doReturn(CATALOG_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(1);
    doReturn(SCHEMA_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(2);
    doReturn(TABLE_NAME_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(3);
    doReturn(COL_NAME_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(4);
    doReturn(KEY_SEQUENCE_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(5);
    doReturn(PRIMARY_KEY_NAME_COLUMN.getResultSetColumnName())
        .when(mockedMetaData)
        .getColumnName(6);
    when(mockedResultSet.getMetaData()).thenReturn(mockedMetaData);
    DatabricksResultSet actualResult =
        metadataClient.listPrimaryKeys(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
    assertEquals(actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED);
    assertEquals(actualResult.getStatementId(), METADATA_STATEMENT_ID);
    assertEquals(((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 1);
  }

  @Test
  void testListImportedKeys() throws Exception {
    DatabricksMetadataSdkClient metadataClient = new DatabricksMetadataSdkClient(mockClient);
    ResultSet resultSet = metadataClient.listImportedKeys(session, "catalog", "schema", "table");
    assertNotNull(resultSet);

    assertEquals(14, resultSet.getMetaData().getColumnCount());
    assertSame("PKTABLE_CAT", resultSet.getMetaData().getColumnName(1));
    assertSame("PKTABLE_SCHEM", resultSet.getMetaData().getColumnName(2));
    assertEquals("PKTABLE_NAME", resultSet.getMetaData().getColumnName(3));
    assertEquals("PKCOLUMN_NAME", resultSet.getMetaData().getColumnName(4));
    assertEquals("FKTABLE_CAT", resultSet.getMetaData().getColumnName(5));
    assertEquals("FKTABLE_SCHEM", resultSet.getMetaData().getColumnName(6));

    assertEquals(1, resultSet.getMetaData().isNullable(1));
    assertEquals(1, resultSet.getMetaData().isNullable(2));
    assertEquals(0, resultSet.getMetaData().isNullable(3));
    assertEquals(0, resultSet.getMetaData().isNullable(4));
    assertEquals(1, resultSet.getMetaData().isNullable(5));
    assertEquals(1, resultSet.getMetaData().isNullable(6));
    assertEquals(0, resultSet.getMetaData().isNullable(7));
    assertEquals(0, resultSet.getMetaData().isNullable(8));
    assertEquals(0, resultSet.getMetaData().isNullable(9));
    assertEquals(1, resultSet.getMetaData().isNullable(10));
    assertEquals(1, resultSet.getMetaData().isNullable(11));
    assertEquals(1, resultSet.getMetaData().isNullable(12));
    assertEquals(1, resultSet.getMetaData().isNullable(13));
    assertEquals(0, resultSet.getMetaData().isNullable(14));

    // Result set is empty
    assertFalse(resultSet.next());
  }

  @Test
  void testListExportedKeys() throws Exception {
    DatabricksMetadataSdkClient metadataClient = new DatabricksMetadataSdkClient(mockClient);

    ResultSet resultSet = metadataClient.listExportedKeys(session, "catalog", "schema", "table");
    assertNotNull(resultSet);

    assertEquals(14, resultSet.getMetaData().getColumnCount());
    assertSame("PKTABLE_CAT", resultSet.getMetaData().getColumnName(1));
    assertSame("PKTABLE_SCHEM", resultSet.getMetaData().getColumnName(2));
    assertEquals("PKTABLE_NAME", resultSet.getMetaData().getColumnName(3));
    assertEquals("PKCOLUMN_NAME", resultSet.getMetaData().getColumnName(4));
    assertEquals("FKTABLE_CAT", resultSet.getMetaData().getColumnName(5));
    assertEquals("FKTABLE_SCHEM", resultSet.getMetaData().getColumnName(6));

    assertEquals(1, resultSet.getMetaData().isNullable(1));
    assertEquals(1, resultSet.getMetaData().isNullable(2));
    assertEquals(0, resultSet.getMetaData().isNullable(3));
    assertEquals(0, resultSet.getMetaData().isNullable(4));
    assertEquals(1, resultSet.getMetaData().isNullable(5));
    assertEquals(1, resultSet.getMetaData().isNullable(6));
    assertEquals(0, resultSet.getMetaData().isNullable(7));
    assertEquals(0, resultSet.getMetaData().isNullable(8));
    assertEquals(0, resultSet.getMetaData().isNullable(9));
    assertEquals(1, resultSet.getMetaData().isNullable(10));
    assertEquals(1, resultSet.getMetaData().isNullable(11));
    assertEquals(1, resultSet.getMetaData().isNullable(12));
    assertEquals(1, resultSet.getMetaData().isNullable(13));
    assertEquals(0, resultSet.getMetaData().isNullable(14));

    // Result set is empty
    assertFalse(resultSet.next());
  }

  @Test
  void testListCrossReferences() throws Exception {
    DatabricksMetadataSdkClient metadataClient = new DatabricksMetadataSdkClient(mockClient);

    ResultSet resultSet =
        metadataClient.listCrossReferences(
            session,
            "primary_catalog",
            "primary_schema",
            "primary_table",
            "foreign_catalog",
            "foreign_schema",
            "foreign_table");
    assertNotNull(resultSet);

    assertEquals(14, resultSet.getMetaData().getColumnCount());
    assertEquals("PKTABLE_CAT", resultSet.getMetaData().getColumnName(1));
    assertEquals("PKTABLE_SCHEM", resultSet.getMetaData().getColumnName(2));
    assertEquals("PKTABLE_NAME", resultSet.getMetaData().getColumnName(3));
    assertEquals("PKCOLUMN_NAME", resultSet.getMetaData().getColumnName(4));
    assertEquals("FKTABLE_CAT", resultSet.getMetaData().getColumnName(5));
    assertEquals("FKTABLE_SCHEM", resultSet.getMetaData().getColumnName(6));
    assertEquals("FKTABLE_NAME", resultSet.getMetaData().getColumnName(7));
    assertEquals("FKCOLUMN_NAME", resultSet.getMetaData().getColumnName(8));
    assertEquals("KEY_SEQ", resultSet.getMetaData().getColumnName(9));
    assertEquals("UPDATE_RULE", resultSet.getMetaData().getColumnName(10));
    assertEquals("DELETE_RULE", resultSet.getMetaData().getColumnName(11));
    assertEquals("FK_NAME", resultSet.getMetaData().getColumnName(12));
    assertEquals("PK_NAME", resultSet.getMetaData().getColumnName(13));
    assertEquals("DEFERRABILITY", resultSet.getMetaData().getColumnName(14));

    // Result set is empty
    assertFalse(resultSet.next());
  }

  @ParameterizedTest
  @MethodSource("listFunctionsTestParams")
  void testTestFunctions(
      String sql, String catalog, String schema, String functionPattern, String description)
      throws SQLException {
    when(session.getComputeResource()).thenReturn(WAREHOUSE_COMPUTE);
    DatabricksMetadataSdkClient metadataClient = new DatabricksMetadataSdkClient(mockClient);
    when(mockClient.executeStatement(
            sql,
            WAREHOUSE_COMPUTE,
            new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.METADATA,
            session,
            null))
        .thenReturn(mockedResultSet);
    when(mockedResultSet.next()).thenReturn(true, false);
    doReturn(6).when(mockedMetaData).getColumnCount();
    doReturn(FUNCTION_NAME_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(1);
    doReturn(FUNCTION_SCHEMA_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(2);
    doReturn(FUNCTION_CATALOG_COLUMN.getResultSetColumnName())
        .when(mockedMetaData)
        .getColumnName(3);
    doReturn(REMARKS_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(4);
    doReturn(FUNCTION_TYPE_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(5);
    doReturn(SPECIFIC_NAME_COLUMN.getResultSetColumnName()).when(mockedMetaData).getColumnName(6);
    when(mockedResultSet.getMetaData()).thenReturn(mockedMetaData);
    DatabricksResultSet actualResult =
        metadataClient.listFunctions(session, catalog, schema, functionPattern);

    assertEquals(
        actualResult.getStatementStatus().getState(), StatementState.SUCCEEDED, description);
    assertEquals(actualResult.getStatementId(), GET_FUNCTIONS_STATEMENT_ID, description);
    assertEquals(
        ((DatabricksResultSetMetaData) actualResult.getMetaData()).getTotalRows(), 1, description);
  }

  @Test
  void testThrowsErrorResultInCaseOfNullCatalog() {
    DatabricksMetadataSdkClient metadataClient = new DatabricksMetadataSdkClient(mockClient);
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
    DatabricksMetadataSdkClient metadataClient = new DatabricksMetadataSdkClient(mockClient);
    assertNotNull(metadataClient.listTypeInfo(session));
  }
}
