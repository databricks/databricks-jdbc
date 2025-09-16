package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class ColumnarRowViewTest {

  @Test
  void testEmptyRowSet() throws DatabricksSQLException {
    TRowSet emptyRowSet = new TRowSet();
    ColumnarRowView view = new ColumnarRowView(emptyRowSet);

    assertEquals(0, view.getRowCount());
    assertEquals(0, view.getColumnCount());
  }

  @Test
  void testNullRowSet() throws DatabricksSQLException {
    ColumnarRowView view = new ColumnarRowView(null);

    assertEquals(0, view.getRowCount());
    assertEquals(0, view.getColumnCount());
  }

  @Test
  void testStringColumn() throws DatabricksSQLException {
    TRowSet rowSet = new TRowSet();
    TColumn stringColumn = new TColumn();
    TStringColumn stringVal = new TStringColumn();
    stringVal.setValues(Arrays.asList("hello", "world", "test"));
    stringColumn.setStringVal(stringVal);

    rowSet.setColumns(Collections.singletonList(stringColumn));

    ColumnarRowView view = new ColumnarRowView(rowSet);

    assertEquals(3, view.getRowCount());
    assertEquals(1, view.getColumnCount());
    assertEquals("hello", view.getValue(0, 0));
    assertEquals("world", view.getValue(1, 0));
    assertEquals("test", view.getValue(2, 0));
  }

  @Test
  void testStringColumnWithNulls() throws DatabricksSQLException {
    TRowSet rowSet = new TRowSet();
    TColumn stringColumn = new TColumn();
    TStringColumn stringVal = new TStringColumn();
    stringVal.setValues(Arrays.asList("hello", "world", "test"));
    // Set second value as null (bit 1 set in byte array)
    stringVal.setNulls(new byte[] {0x02}); // Binary: 00000010 (bit 1 is set)
    stringColumn.setStringVal(stringVal);

    rowSet.setColumns(Collections.singletonList(stringColumn));

    ColumnarRowView view = new ColumnarRowView(rowSet);

    assertEquals(3, view.getRowCount());
    assertEquals(1, view.getColumnCount());
    assertEquals("hello", view.getValue(0, 0));
    assertNull(view.getValue(1, 0));
    assertEquals("test", view.getValue(2, 0));
  }

  @Test
  void testIntegerColumn() throws DatabricksSQLException {
    TRowSet rowSet = new TRowSet();
    TColumn intColumn = new TColumn();
    TI32Column intVal = new TI32Column();
    intVal.setValues(Arrays.asList(10, 20, 30));
    intColumn.setI32Val(intVal);

    rowSet.setColumns(Collections.singletonList(intColumn));

    ColumnarRowView view = new ColumnarRowView(rowSet);

    assertEquals(3, view.getRowCount());
    assertEquals(1, view.getColumnCount());
    assertEquals(10, view.getValue(0, 0));
    assertEquals(20, view.getValue(1, 0));
    assertEquals(30, view.getValue(2, 0));
  }

  @Test
  void testMultipleColumns() throws DatabricksSQLException {
    TRowSet rowSet = new TRowSet();

    // String column
    TColumn stringColumn = new TColumn();
    TStringColumn stringVal = new TStringColumn();
    stringVal.setValues(Arrays.asList("a", "b"));
    stringColumn.setStringVal(stringVal);

    // Integer column
    TColumn intColumn = new TColumn();
    TI32Column intVal = new TI32Column();
    intVal.setValues(Arrays.asList(1, 2));
    intColumn.setI32Val(intVal);

    rowSet.setColumns(Arrays.asList(stringColumn, intColumn));

    ColumnarRowView view = new ColumnarRowView(rowSet);

    assertEquals(2, view.getRowCount());
    assertEquals(2, view.getColumnCount());

    // First row
    assertEquals("a", view.getValue(0, 0));
    assertEquals(1, view.getValue(0, 1));

    // Second row
    assertEquals("b", view.getValue(1, 0));
    assertEquals(2, view.getValue(1, 1));
  }

  @Test
  void testMaterializeRow() throws DatabricksSQLException {
    TRowSet rowSet = new TRowSet();

    // String column
    TColumn stringColumn = new TColumn();
    TStringColumn stringVal = new TStringColumn();
    stringVal.setValues(Arrays.asList("hello", "world"));
    stringColumn.setStringVal(stringVal);

    // Integer column
    TColumn intColumn = new TColumn();
    TI32Column intVal = new TI32Column();
    intVal.setValues(Arrays.asList(100, 200));
    intColumn.setI32Val(intVal);

    rowSet.setColumns(Arrays.asList(stringColumn, intColumn));

    ColumnarRowView view = new ColumnarRowView(rowSet);

    Object[] row0 = view.materializeRow(0);
    assertArrayEquals(new Object[] {"hello", 100}, row0);

    Object[] row1 = view.materializeRow(1);
    assertArrayEquals(new Object[] {"world", 200}, row1);
  }

  @Test
  void testOutOfBoundsAccess() throws DatabricksSQLException {
    TRowSet rowSet = new TRowSet();
    TColumn stringColumn = new TColumn();
    TStringColumn stringVal = new TStringColumn();
    stringVal.setValues(Arrays.asList("test"));
    stringColumn.setStringVal(stringVal);
    rowSet.setColumns(Collections.singletonList(stringColumn));

    ColumnarRowView view = new ColumnarRowView(rowSet);

    // Row out of bounds
    assertThrows(DatabricksSQLException.class, () -> view.getValue(-1, 0));
    assertThrows(DatabricksSQLException.class, () -> view.getValue(1, 0));

    // Column out of bounds
    assertThrows(DatabricksSQLException.class, () -> view.getValue(0, -1));
    assertThrows(DatabricksSQLException.class, () -> view.getValue(0, 1));

    // Materialize row out of bounds
    assertThrows(DatabricksSQLException.class, () -> view.materializeRow(-1));
    assertThrows(DatabricksSQLException.class, () -> view.materializeRow(1));
  }
}
