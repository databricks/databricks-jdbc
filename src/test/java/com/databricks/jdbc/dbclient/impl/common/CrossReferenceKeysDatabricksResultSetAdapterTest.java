package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.common.MetadataResultConstants.*;
import static com.databricks.jdbc.common.MetadataResultConstants.PKCOLUMN_NAME;
import static com.databricks.jdbc.dbclient.impl.common.ImportedKeysDatabricksResultSetAdapter.PARENT_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.model.core.ResultColumn;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class CrossReferenceKeysDatabricksResultSetAdapterTest {

  private static final String TARGET_PARENT_TABLE_NAME = "targetTable";
  private IDatabricksResultSetAdapter crossRefAdapter;

  @Mock private ResultSet mockResultSet;

  @BeforeEach
  public void setUp() {
    crossRefAdapter = new CrossReferenceKeysDatabricksResultSetAdapter(TARGET_PARENT_TABLE_NAME);
  }

  @Test
  public void testIncludeRowWhenParentTableNameMatches() throws SQLException {
    List<ResultColumn> columns = new ArrayList<>();

    // Mock the ResultSet to return the matching target parent table name
    // The result set contains the column name "pktableName" which is mapped to PARENT_TABLE_NAME
    when(mockResultSet.getString(PARENT_TABLE_NAME.getResultSetColumnName()))
        .thenReturn(TARGET_PARENT_TABLE_NAME);

    boolean result = crossRefAdapter.includeRow(mockResultSet, columns);

    assertTrue(result, "includeRow should return true when parent table name matches");
    verify(mockResultSet).getString(PARENT_TABLE_NAME.getResultSetColumnName());
  }

  @Test
  public void testIncludeRowWhenParentTableNameDoesNotMatch() throws SQLException {
    List<ResultColumn> columns = new ArrayList<>();

    // Mock the ResultSet to return a non-matching parent table name
    when(mockResultSet.getString(PARENT_TABLE_NAME.getResultSetColumnName()))
        .thenReturn("differentTable");

    boolean result = crossRefAdapter.includeRow(mockResultSet, columns);

    assertFalse(result, "includeRow should return false when parent table name doesn't match");
    verify(mockResultSet).getString(PARENT_TABLE_NAME.getResultSetColumnName());
  }

  @Test
  public void testIncludeRowWithNullResultSet() {
    List<ResultColumn> columns = new ArrayList<>();

    assertThrows(
        NullPointerException.class,
        () -> crossRefAdapter.includeRow(null, columns),
        "includeRow should throw NullPointerException when ResultSet is null");
  }

  @Test
  public void testIncludeRowHandlesSQLException() throws SQLException {
    List<ResultColumn> columns = new ArrayList<>();

    // Mock the ResultSet to throw SQLException when getString is called
    when(mockResultSet.getString(PARENT_TABLE_NAME.getResultSetColumnName()))
        .thenThrow(new SQLException("Test exception"));

    assertThrows(
        SQLException.class,
        () -> crossRefAdapter.includeRow(mockResultSet, columns),
        "includeRow should propagate SQLException from ResultSet");
  }

  @Test
  public void testConstructorSetsTargetParentTableName() throws SQLException {
    String customTableName = "customTable";
    CrossReferenceKeysDatabricksResultSetAdapter customAdapter =
        new CrossReferenceKeysDatabricksResultSetAdapter(customTableName);

    // Mock the ResultSet to return the matching custom table name
    when(mockResultSet.getString(PARENT_TABLE_NAME.getResultSetColumnName()))
        .thenReturn(customTableName);

    boolean result = customAdapter.includeRow(mockResultSet, new ArrayList<>());

    assertTrue(result, "Constructor should properly set the target parent table name");
  }

  @Test
  public void testInheritanceFromImportedKeysAdapter() {
    assertInstanceOf(ImportedKeysDatabricksResultSetAdapter.class, crossRefAdapter);
  }

  @Test
  public void testMapColumnWithPKTABLE_CAT() {
    ResultColumn column =
        new ResultColumn("someLabel", PKTABLE_CAT.getResultSetColumnName(), Types.VARCHAR);

    ResultColumn result = crossRefAdapter.mapColumn(column);

    assertEquals("PKTABLE_CAT", result.getColumnName());
    assertEquals("parentCatalogName", result.getResultSetColumnName());
    assertEquals(Types.VARCHAR, result.getColumnTypeInt());
  }

  @Test
  public void testMapColumnWithPKTABLE_SCHEM() {
    ResultColumn column =
        new ResultColumn("someLabel", PKTABLE_SCHEM.getResultSetColumnName(), Types.VARCHAR);

    ResultColumn result = crossRefAdapter.mapColumn(column);

    assertEquals("PKTABLE_SCHEM", result.getColumnName());
    assertEquals("parentNamespace", result.getResultSetColumnName());
    assertEquals(Types.VARCHAR, result.getColumnTypeInt());
  }

  @Test
  public void testMapColumnWithPKTABLE_NAME() {
    ResultColumn column =
        new ResultColumn("someLabel", PKTABLE_NAME.getResultSetColumnName(), Types.VARCHAR);

    ResultColumn result = crossRefAdapter.mapColumn(column);

    assertEquals("PKTABLE_NAME", result.getColumnName());
    assertEquals("parentTableName", result.getResultSetColumnName());
    assertEquals(Types.VARCHAR, result.getColumnTypeInt());
    assertEquals(ImportedKeysDatabricksResultSetAdapter.PARENT_TABLE_NAME, result);
  }

  @Test
  public void testMapColumnWithPKCOLUMN_NAME() {
    ResultColumn column =
        new ResultColumn("someLabel", PKCOLUMN_NAME.getResultSetColumnName(), Types.VARCHAR);

    ResultColumn result = crossRefAdapter.mapColumn(column);

    assertEquals("PKCOLUMN_NAME", result.getColumnName());
    assertEquals("parentColName", result.getResultSetColumnName());
    assertEquals(Types.VARCHAR, result.getColumnTypeInt());
  }
}
