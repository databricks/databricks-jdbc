package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TColumn;
import com.databricks.jdbc.model.client.thrift.generated.TRowSet;
import com.databricks.jdbc.model.core.ColumnVector;
import com.databricks.jdbc.model.core.ColumnarData;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * Memory-efficient columnar view that provides row-based access without materializing all rows.
 * Instead of creating {@code List<List<Object>>}, this class provides direct access to columnar
 * data on a per-row, per-column basis, significantly reducing memory allocations.
 */
public class ColumnarRowView {
  private final int columnCount;
  private final int rowCount;
  private final ColumnAccessor[] columnAccessors;

  public ColumnarRowView(TRowSet rowSet) throws DatabricksSQLException {
    List<TColumn> columns = rowSet != null ? rowSet.getColumns() : null;

    if (columns == null || columns.isEmpty()) {
      this.columnCount = 0;
      this.rowCount = 0;
      this.columnAccessors = new ColumnAccessor[0];
    } else {
      this.columnCount = columns.size();
      this.rowCount = getColumnSize(columns.get(0));
      this.columnAccessors = new ColumnAccessor[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        this.columnAccessors[i] = createColumnAccessor(columns.get(i));
      }
    }
  }

  public ColumnarRowView(ColumnarData columnarData) throws DatabricksSQLException {
    List<ColumnVector> columns =
        columnarData == null || columnarData.getColumns() == null
            ? Collections.emptyList()
            : columnarData.getColumns();
    this.columnCount = columns.size();
    this.rowCount = columns.isEmpty() ? 0 : getColumnSize(columns.get(0));
    this.columnAccessors = new ColumnAccessor[columnCount];
    for (int i = 0; i < columnCount; i++) {
      if (getColumnSize(columns.get(i)) != rowCount) {
        throw new DatabricksSQLException(
            "Columnar result has inconsistent vector lengths",
            DatabricksDriverErrorCode.INVALID_STATE);
      }
      this.columnAccessors[i] = createColumnAccessor(columns.get(i));
    }
  }

  /** Gets the number of rows in this view. */
  public int getRowCount() {
    return rowCount;
  }

  /** Gets the number of columns in this view. */
  public int getColumnCount() {
    return columnCount;
  }

  /** Gets the value at the specified row and column without materializing the entire row. */
  public Object getValue(int rowIndex, int columnIndex) throws DatabricksSQLException {
    if (rowIndex < 0 || rowIndex >= rowCount) {
      throw new DatabricksSQLException(
          "Row index out of bounds: " + rowIndex, DatabricksDriverErrorCode.INVALID_STATE);
    }
    if (columnIndex < 0 || columnIndex >= columnAccessors.length) {
      throw new DatabricksSQLException(
          "Column index out of bounds: " + columnIndex, DatabricksDriverErrorCode.INVALID_STATE);
    }

    return columnAccessors[columnIndex].getValue(rowIndex);
  }

  /**
   * Creates a materialized row only when explicitly requested (for backward compatibility). This
   * should be avoided in performance-critical paths.
   */
  public Object[] materializeRow(int rowIndex) throws DatabricksSQLException {
    if (rowIndex < 0 || rowIndex >= rowCount) {
      throw new DatabricksSQLException(
          "Row index out of bounds: " + rowIndex, DatabricksDriverErrorCode.INVALID_STATE);
    }

    Object[] row = new Object[columnAccessors.length];
    for (int col = 0; col < columnAccessors.length; col++) {
      row[col] = columnAccessors[col].getValue(rowIndex);
    }
    return row;
  }

  private static int getColumnSize(TColumn column) throws DatabricksSQLException {
    if (column.isSetBinaryVal()) return column.getBinaryVal().getValuesSize();
    if (column.isSetBoolVal()) return column.getBoolVal().getValuesSize();
    if (column.isSetByteVal()) return column.getByteVal().getValuesSize();
    if (column.isSetDoubleVal()) return column.getDoubleVal().getValuesSize();
    if (column.isSetI16Val()) return column.getI16Val().getValuesSize();
    if (column.isSetI32Val()) return column.getI32Val().getValuesSize();
    if (column.isSetI64Val()) return column.getI64Val().getValuesSize();
    if (column.isSetStringVal()) return column.getStringVal().getValuesSize();

    throw new DatabricksSQLException(
        "Unsupported column type: " + column, DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  private static int getColumnSize(ColumnVector column) throws DatabricksSQLException {
    if (column == null || column.getValues() == null) {
      throw new DatabricksSQLException(
          "Unsupported columnar values arm", DatabricksDriverErrorCode.INVALID_STATE);
    }
    return column.getValues().size();
  }

  private static ColumnAccessor createColumnAccessor(TColumn column) throws DatabricksSQLException {
    if (column.isSetBinaryVal()) {
      return new TypedColumnAccessor<>(
          column.getBinaryVal().getValues(), column.getBinaryVal().getNulls());
    }
    if (column.isSetBoolVal()) {
      return new TypedColumnAccessor<>(
          column.getBoolVal().getValues(), column.getBoolVal().getNulls());
    }
    if (column.isSetByteVal()) {
      return new TypedColumnAccessor<>(
          column.getByteVal().getValues(), column.getByteVal().getNulls());
    }
    if (column.isSetDoubleVal()) {
      return new TypedColumnAccessor<>(
          column.getDoubleVal().getValues(), column.getDoubleVal().getNulls());
    }
    if (column.isSetI16Val()) {
      return new TypedColumnAccessor<>(
          column.getI16Val().getValues(), column.getI16Val().getNulls());
    }
    if (column.isSetI32Val()) {
      return new TypedColumnAccessor<>(
          column.getI32Val().getValues(), column.getI32Val().getNulls());
    }
    if (column.isSetI64Val()) {
      return new TypedColumnAccessor<>(
          column.getI64Val().getValues(), column.getI64Val().getNulls());
    }
    if (column.isSetStringVal()) {
      return new TypedColumnAccessor<>(
          column.getStringVal().getValues(), column.getStringVal().getNulls());
    }

    throw new DatabricksSQLException(
        "Unsupported column type: " + column, DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  private static ColumnAccessor createColumnAccessor(ColumnVector column)
      throws DatabricksSQLException {
    if (column.getBoolValues() != null) {
      return new TypedColumnAccessor<>(
          column.getBoolValues().getValues(), column.getNullMask());
    }
    if (column.getByteValues() != null) {
      return new ByteColumnAccessor(column.getByteValues().getValues(), column.getNullMask());
    }
    if (column.getI16Values() != null) {
      return new TypedColumnAccessor<>(
          column.getI16Values().getValues(), column.getNullMask());
    }
    if (column.getI32Values() != null) {
      return new TypedColumnAccessor<>(
          column.getI32Values().getValues(), column.getNullMask());
    }
    if (column.getI64Values() != null) {
      return new TypedColumnAccessor<>(
          column.getI64Values().getValues(), column.getNullMask());
    }
    if (column.getDoubleValues() != null) {
      return new TypedColumnAccessor<>(
          column.getDoubleValues().getValues(), column.getNullMask());
    }
    if (column.getStringValues() != null) {
      return new TypedColumnAccessor<>(
          column.getStringValues().getValues(), column.getNullMask());
    }
    if (column.getBinaryValues() != null) {
      return new TypedColumnAccessor<>(
          column.getBinaryValues().getValues(), column.getNullMask());
    }
    throw new DatabricksSQLException(
        "Unsupported columnar values arm", DatabricksDriverErrorCode.INVALID_STATE);
  }

  /** Interface for accessing column values by index without materializing the entire column. */
  private interface ColumnAccessor {
    Object getValue(int rowIndex);
  }

  /** Memory-efficient column accessor that handles nulls and provides direct index-based access. */
  private static class TypedColumnAccessor<T> implements ColumnAccessor {
    private final List<T> values;
    private final BitSet nullBits;

    public TypedColumnAccessor(List<T> values, byte[] nulls) {
      this.values = values;
      this.nullBits = nulls != null ? BitSet.valueOf(nulls) : null;
    }

    @Override
    public Object getValue(int rowIndex) {
      if (nullBits != null && nullBits.get(rowIndex)) {
        return null;
      }
      return values.get(rowIndex);
    }
  }

  private static class ByteColumnAccessor extends TypedColumnAccessor<Integer> {
    ByteColumnAccessor(List<Integer> values, byte[] nulls) {
      super(values, nulls);
    }

    @Override
    public Object getValue(int rowIndex) {
      Object value = super.getValue(rowIndex);
      return value == null ? null : ((Integer) value).byteValue();
    }
  }
}
