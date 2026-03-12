package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.model.core.ResultColumn;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DefaultDatabricksResultSetAdapterTest {

  private DefaultDatabricksResultSetAdapter adapter;

  @Mock private ResultSet mockResultSet;

  @Mock private ResultColumn mockColumn;

  @BeforeEach
  public void setUp() {
    adapter = new DefaultDatabricksResultSetAdapter();
  }

  @Test
  public void testMapColumnReturnsColumnUnchanged() {
    ResultColumn result = adapter.mapColumn(mockColumn);
    assertSame(mockColumn, result, "mapColumn should return the exact same column object");
  }

  @Test
  public void testMapColumnHandlesNull() {
    assertNull(adapter.mapColumn(null), "mapColumn should return null when given null");
  }

  @Test
  public void testIncludeRowAlwaysReturnsTrue() throws SQLException {
    List<ResultColumn> columns = new ArrayList<>();
    columns.add(mockColumn);

    boolean result = adapter.includeRow(mockResultSet, columns);

    assertTrue(result, "includeRow should always return true");
    verifyNoInteractions(mockResultSet, mockColumn);
  }

  @Test
  public void testIncludeRowWithNullResultSet() throws SQLException {
    List<ResultColumn> columns = new ArrayList<>();
    columns.add(mockColumn);

    boolean result = adapter.includeRow(null, columns);
    assertTrue(result, "includeRow should return true even with null ResultSet");
  }

  @Test
  public void testIncludeRowWithNullColumnList() throws SQLException {
    assertTrue(
        adapter.includeRow(mockResultSet, null),
        "includeRow should return true even with null column list");
  }
}
