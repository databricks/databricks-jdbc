package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.common.MetadataResultConstants.CATALOG_FULL_COLUMN;
import static com.databricks.jdbc.common.MetadataResultConstants.CATALOG_RESULT_COLUMN;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.model.core.ResultColumn;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SchemasDatabricksResultSetAdapterTest {

  private SchemasDatabricksResultSetAdapter adapter;
  private ResultSet mockResultSet;

  @BeforeEach
  void setUp() {
    adapter = new SchemasDatabricksResultSetAdapter();
    mockResultSet = mock(ResultSet.class);
  }

  @Test
  void mapColumn_shouldMapCatalogFullColumnToCatalogColumnForGetCatalogs() {
    ResultColumn inputColumn = mock(ResultColumn.class);
    when(inputColumn.getResultSetColumnName())
        .thenReturn(CATALOG_FULL_COLUMN.getResultSetColumnName());

    ResultColumn result = adapter.mapColumn(inputColumn);

    assertSame(CATALOG_RESULT_COLUMN, result);
  }

  @Test
  void mapColumn_shouldReturnSameColumnForNonCatalogFullColumn() {
    ResultColumn inputColumn = mock(ResultColumn.class);
    when(inputColumn.getResultSetColumnName()).thenReturn("some_other_column");

    ResultColumn result = adapter.mapColumn(inputColumn);

    assertSame(inputColumn, result);
  }

  @Test
  void includeRow_shouldAlwaysReturnTrue() throws SQLException {
    List<ResultColumn> columns = Arrays.asList(mock(ResultColumn.class), mock(ResultColumn.class));

    boolean result = adapter.includeRow(mockResultSet, columns);

    assertTrue(result);
  }
}
