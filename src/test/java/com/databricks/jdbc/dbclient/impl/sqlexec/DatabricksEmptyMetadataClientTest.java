package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksEmptyMetadataClientTest {

  @Mock private IDatabricksSession session;
  @Mock private IDatabricksConnectionContext ctx;
  private DatabricksEmptyMetadataClient mockClient;

  @BeforeEach
  void setUp() {
    mockClient = new DatabricksEmptyMetadataClient(ctx);
  }

  @Test
  void testListTypeInfo() throws SQLException {
    ResultSet resultSet = mockClient.listTypeInfo(session);
    assertNotNull(resultSet);
    assertEquals(resultSet.getMetaData().getColumnCount(), 18);
  }

  @Test
  void testListCatalogs() throws SQLException {
    ResultSet resultSet = mockClient.listCatalogs(session);
    assertNotNull(resultSet);
    assertFalse(resultSet.next()); // empty result set
    assertEquals(resultSet.getMetaData().getColumnCount(), 1);
    assertEquals(resultSet.getMetaData().getColumnName(1), "TABLE_CAT");
  }

  @Test
  void testListSchemas() throws SQLException {
    ResultSet resultSet = mockClient.listSchemas(session, "catalog", "schemaNamePattern");
    assertNotNull(resultSet);
    assertFalse(resultSet.next()); // empty result set
    assertEquals(resultSet.getMetaData().getColumnCount(), 2);
    assertEquals(resultSet.getMetaData().getColumnName(1), "TABLE_SCHEM");
    assertEquals(resultSet.getMetaData().getColumnName(2), "TABLE_CATALOG");
  }

  @Test
  void testListTables() throws SQLException {
    ResultSet resultSet =
        mockClient.listTables(
            session,
            "catalog",
            "schemaNamePattern",
            "tableNamePattern",
            new String[] {"tableTypes"});
    assertNotNull(resultSet);
    assertFalse(resultSet.next()); // empty result set
    assertEquals(resultSet.getMetaData().getColumnCount(), 10);
    assertEquals(resultSet.getMetaData().getColumnName(1), "TABLE_CAT");
    assertEquals(resultSet.getMetaData().getColumnName(2), "TABLE_SCHEM");
    assertEquals(resultSet.getMetaData().getColumnName(3), "TABLE_NAME");
  }

  @Test
  void testListTableTypes() throws SQLException {
    when(ctx.getEnableMetricViewMetadata()).thenReturn(false);

    ResultSet resultSet = mockClient.listTableTypes(session);
    assertNotNull(resultSet);
    assertEquals(resultSet.getMetaData().getColumnCount(), 1);
    assertEquals(resultSet.getMetaData().getColumnName(1), "TABLE_TYPE");
  }

  @Test
  void testListColumns() throws SQLException {
    ResultSet resultSet =
        mockClient.listColumns(
            session, "catalog", "schemaNamePattern", "tableNamePattern", "columnNamePattern");
    assertNotNull(resultSet);
    assertFalse(resultSet.next()); // empty result set
    assertEquals(resultSet.getMetaData().getColumnCount(), 24);
    assertEquals(resultSet.getMetaData().getColumnName(1), "TABLE_CAT");
    assertEquals(resultSet.getMetaData().getColumnName(2), "TABLE_SCHEM");
    assertEquals(resultSet.getMetaData().getColumnName(3), "TABLE_NAME");
    assertEquals(resultSet.getMetaData().getColumnName(4), "COLUMN_NAME");
  }

  @Test
  void testListFunctions() throws SQLException {
    ResultSet resultSet =
        mockClient.listFunctions(session, "catalog", "schemaNamePattern", "functionNamePattern");
    assertNotNull(resultSet);
    assertFalse(resultSet.next()); // empty result set
    assertEquals(resultSet.getMetaData().getColumnCount(), 6);
    assertEquals(resultSet.getMetaData().getColumnName(1), "FUNCTION_CAT");
    assertEquals(resultSet.getMetaData().getColumnName(2), "FUNCTION_SCHEM");
    assertEquals(resultSet.getMetaData().getColumnName(3), "FUNCTION_NAME");
  }

  @Test
  void testListPrimaryKeys() throws SQLException {
    ResultSet resultSet =
        mockClient.listPrimaryKeys(session, "catalog", "schemaNamePattern", "tableNamePattern");
    assertNotNull(resultSet);
    assertFalse(resultSet.next()); // empty result set
    assertEquals(resultSet.getMetaData().getColumnCount(), 6);
    assertEquals(resultSet.getMetaData().getColumnName(1), "TABLE_CAT");
    assertEquals(resultSet.getMetaData().getColumnName(2), "TABLE_SCHEM");
    assertEquals(resultSet.getMetaData().getColumnName(3), "TABLE_NAME");
  }

  @Test
  void testListImportedKeys() throws SQLException {
    ResultSet resultSet = mockClient.listImportedKeys(session, "catalog", "schema", "table");
    assertNotNull(resultSet);
    assertFalse(resultSet.next()); // empty result set
    assertEquals(14, resultSet.getMetaData().getColumnCount());
    assertEquals("PKTABLE_CAT", resultSet.getMetaData().getColumnName(1));
  }

  @Test
  void testListExportedKeys() throws SQLException {
    ResultSet resultSet = mockClient.listExportedKeys(session, "catalog", "schema", "table");
    assertNotNull(resultSet);
    assertFalse(resultSet.next()); // empty result set
    assertEquals(14, resultSet.getMetaData().getColumnCount());
    assertEquals("PKTABLE_CAT", resultSet.getMetaData().getColumnName(1));
  }

  @Test
  void testListCrossReferences() throws SQLException {
    ResultSet resultSet =
        mockClient.listCrossReferences(
            session,
            "parentCatalog",
            "parentSchema",
            "parentTable",
            "foreignCatalog",
            "foreignSchema",
            "foreignTable");
    assertNotNull(resultSet);
    assertFalse(resultSet.next()); // empty result set
    assertEquals(14, resultSet.getMetaData().getColumnCount());
    assertEquals("PKTABLE_CAT", resultSet.getMetaData().getColumnName(1));
  }
}
