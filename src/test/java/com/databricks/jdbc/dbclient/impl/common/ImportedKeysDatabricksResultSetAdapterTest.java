package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.common.MetadataResultConstants.*;
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
public class ImportedKeysDatabricksResultSetAdapterTest {

  private IDatabricksResultSetAdapter adapter;

  @Mock private ResultSet mockResultSet;

  @BeforeEach
  public void setUp() {
    adapter = new ImportedKeysDatabricksResultSetAdapter();
  }

  @Test
  public void testMapColumnWithPKTABLE_CAT() {
    ResultColumn column =
        new ResultColumn("someLabel", PKTABLE_CAT.getResultSetColumnName(), Types.VARCHAR);

    ResultColumn result = adapter.mapColumn(column);

    assertEquals("PKTABLE_CAT", result.getColumnName());
    assertEquals("parentCatalogName", result.getResultSetColumnName());
    assertEquals(Types.VARCHAR, result.getColumnTypeInt());
  }

  @Test
  public void testMapColumnWithPKTABLE_SCHEM() {
    ResultColumn column =
        new ResultColumn("someLabel", PKTABLE_SCHEM.getResultSetColumnName(), Types.VARCHAR);

    ResultColumn result = adapter.mapColumn(column);

    assertEquals("PKTABLE_SCHEM", result.getColumnName());
    assertEquals("parentNamespace", result.getResultSetColumnName());
    assertEquals(Types.VARCHAR, result.getColumnTypeInt());
  }

  @Test
  public void testMapColumnWithPKTABLE_NAME() {
    ResultColumn column =
        new ResultColumn("someLabel", PKTABLE_NAME.getResultSetColumnName(), Types.VARCHAR);

    ResultColumn result = adapter.mapColumn(column);

    assertEquals("PKTABLE_NAME", result.getColumnName());
    assertEquals("parentTableName", result.getResultSetColumnName());
    assertEquals(Types.VARCHAR, result.getColumnTypeInt());
    assertEquals(ImportedKeysDatabricksResultSetAdapter.PARENT_TABLE_NAME, result);
  }

  @Test
  public void testMapColumnWithPKCOLUMN_NAME() {
    ResultColumn column =
        new ResultColumn("someLabel", PKCOLUMN_NAME.getResultSetColumnName(), Types.VARCHAR);

    ResultColumn result = adapter.mapColumn(column);

    assertEquals("PKCOLUMN_NAME", result.getColumnName());
    assertEquals("parentColName", result.getResultSetColumnName());
    assertEquals(Types.VARCHAR, result.getColumnTypeInt());
  }

  @Test
  public void testMapColumnWithFKTABLE_CAT() {
    ResultColumn column =
        new ResultColumn("someLabel", FKTABLE_CAT.getResultSetColumnName(), Types.VARCHAR);

    ResultColumn result = adapter.mapColumn(column);

    assertEquals(CATALOG_COLUMN, result);
  }

  @Test
  public void testMapColumnWithFKTABLE_SCHEM() {
    ResultColumn column =
        new ResultColumn("someLabel", FKTABLE_SCHEM.getResultSetColumnName(), Types.VARCHAR);

    ResultColumn result = adapter.mapColumn(column);

    assertEquals(SCHEMA_COLUMN, result);
  }

  @Test
  public void testMapColumnWithFKTABLE_NAME() {
    ResultColumn column =
        new ResultColumn("someLabel", FKTABLE_NAME.getResultSetColumnName(), Types.VARCHAR);

    ResultColumn result = adapter.mapColumn(column);

    assertEquals(TABLE_NAME_COLUMN, result);
  }

  @Test
  public void testMapColumnWithFKCOLUMN_NAME() {
    ResultColumn column =
        new ResultColumn("someLabel", FKCOLUMN_NAME.getResultSetColumnName(), Types.VARCHAR);

    ResultColumn result = adapter.mapColumn(column);

    assertEquals(COL_NAME_COLUMN, result);
  }

  @Test
  public void testMapColumnWithFK_NAME() {
    ResultColumn column =
        new ResultColumn("someLabel", FK_NAME.getResultSetColumnName(), Types.VARCHAR);

    ResultColumn result = adapter.mapColumn(column);

    assertEquals("FK_NAME", result.getColumnName());
    assertEquals("constraintName", result.getResultSetColumnName());
    assertEquals(Types.VARCHAR, result.getColumnTypeInt());
    assertEquals(ImportedKeysDatabricksResultSetAdapter.FOREIGN_KEY_NAME_COLUMN, result);
  }

  @Test
  public void testMapColumnWithUnmappedColumn() {
    ResultColumn unmappedColumn = new ResultColumn("UNMAPPED_COLUMN", "unmapped", Types.INTEGER);

    ResultColumn result = adapter.mapColumn(unmappedColumn);

    assertSame(unmappedColumn, result, "Unmapped columns should be returned as-is");
  }

  @Test
  public void testIncludeRowAlwaysReturnsTrue() throws SQLException {
    List<ResultColumn> columns = new ArrayList<>();
    columns.add(new ResultColumn("TEST", "test", Types.VARCHAR));

    boolean result = adapter.includeRow(mockResultSet, columns);

    assertTrue(result, "includeRow should always return true");
    verifyNoInteractions(mockResultSet);
  }

  @Test
  public void testIncludeRowWithEmptyColumnList() throws SQLException {
    List<ResultColumn> emptyColumns = new ArrayList<>();

    boolean result = adapter.includeRow(mockResultSet, emptyColumns);

    assertTrue(result, "includeRow should return true even with empty column list");
  }

  @Test
  public void testIncludeRowWithNulls() throws SQLException {
    assertTrue(
        adapter.includeRow(null, null), "includeRow should return true even with null parameters");
  }
}
