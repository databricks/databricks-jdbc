package com.databricks.jdbc.client.impl.thrift.commons;

import static com.databricks.jdbc.client.impl.helper.MetadataResultConstants.NULL_STRING;
import static com.databricks.jdbc.client.impl.thrift.generated.TTypeId.TIMESTAMP_TYPE;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksThriftHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksThriftHelper.class);
  public static final List<TStatusCode> SUCCESS_STATUS_LIST =
      List.of(TStatusCode.SUCCESS_STATUS, TStatusCode.SUCCESS_WITH_INFO_STATUS);

  public static TNamespace getNamespace(String catalog, String schema) {
    return new TNamespace().setCatalogName(catalog).setSchemaName(schema);
  }

  public static String byteBufferToString(ByteBuffer buffer) {
    ByteBuffer newBuffer = buffer.duplicate(); // This is to avoid a BufferUnderflowException
    long sigBits = newBuffer.getLong();
    return new UUID(sigBits, sigBits).toString();
  }

  public static void verifySuccessStatus(TStatusCode statusCode, String errorContext)
      throws DatabricksHttpException {
    if (!SUCCESS_STATUS_LIST.contains(statusCode)) {
      String errorMessage = "Error thrift response received. " + errorContext;
      LOGGER.error(errorMessage);
      throw new DatabricksHttpException(errorMessage);
    }
  }

  public static int getColumnCount(TGetResultSetMetadataResp resultManifest) {
    if (resultManifest == null || resultManifest.getSchema() == null) {
      return 0;
    }
    return resultManifest.getSchema().getColumnsSize();
  }

  public static List<List<Object>> extractValues(List<TColumn> columnList) {
    if (columnList == null) {
      return Collections.singletonList(Collections.emptyList());
    }
    List<Object> obj =
        columnList.stream()
            .map(
                column -> {
                  try {
                    return getColumnFirstValue(column);
                  } catch (Exception e) {
                    // In case a column doesn't have an object, add the default null value
                    return NULL_STRING;
                  }
                })
            .collect(Collectors.toList());
    return Collections.singletonList(obj);
  }

  private static Object getColumnFirstValue(TColumn column) {
    return getColumnValues(column).get(0);
  }

  public static ColumnInfoTypeName getTypeFromTypeDesc(TTypeDesc typeDesc) {
    TTypeId type =
        Optional.ofNullable(typeDesc)
            .map(TTypeDesc::getTypes)
            .map(t -> t.get(0))
            .map(TTypeEntry::getPrimitiveEntry)
            .map(TPrimitiveTypeEntry::getType)
            .orElse(TTypeId.STRING_TYPE);
    switch (type) {
      case BOOLEAN_TYPE:
        return ColumnInfoTypeName.BOOLEAN;
      case TINYINT_TYPE:
      case SMALLINT_TYPE:
        return ColumnInfoTypeName.SHORT;
      case INT_TYPE:
        return ColumnInfoTypeName.INT;
      case BIGINT_TYPE:
        return ColumnInfoTypeName.LONG;
      case FLOAT_TYPE:
        return ColumnInfoTypeName.FLOAT;
      case DOUBLE_TYPE:
        return ColumnInfoTypeName.DOUBLE;
      case VARCHAR_TYPE:
      case STRING_TYPE:
        return ColumnInfoTypeName.STRING;
      case TIMESTAMP_TYPE:
        return ColumnInfoTypeName.TIMESTAMP;
      case BINARY_TYPE:
        return ColumnInfoTypeName.BINARY;
      case DECIMAL_TYPE:
        return ColumnInfoTypeName.DECIMAL;
      case NULL_TYPE:
        return ColumnInfoTypeName.NULL;
      case DATE_TYPE:
        return ColumnInfoTypeName.DATE;
      case CHAR_TYPE:
        return ColumnInfoTypeName.CHAR;
      case INTERVAL_YEAR_MONTH_TYPE:
      case INTERVAL_DAY_TIME_TYPE:
        return ColumnInfoTypeName.INTERVAL;
    }
    return ColumnInfoTypeName.STRING; // by default return string
  }

  private static List<?> getColumnValues(TColumn column) {
    // TODO : Handle complex data types
    if (column.isSetBinaryVal()) return column.getBinaryVal().getValues();
    if (column.isSetBoolVal()) return column.getBoolVal().getValues();
    if (column.isSetByteVal()) return column.getByteVal().getValues();
    if (column.isSetDoubleVal()) return column.getDoubleVal().getValues();
    if (column.isSetI16Val()) return column.getI16Val().getValues();
    if (column.isSetI32Val()) return column.getI32Val().getValues();
    if (column.isSetI64Val()) return column.getI64Val().getValues();
    return column.getStringVal().getValues(); // Default case
  }

  public static List<List<Object>> convertColumnarToRowBased(TRowSet rowSet) {
    List<List<Object>> columnarData = extractValuesFromRowSet(rowSet);
    if (columnarData.isEmpty()) {
      return Collections.emptyList();
    }
    int numRows = columnarData.get(0).size();
    List<List<Object>> rowBasedData =
        IntStream.range(0, numRows)
            .mapToObj(i -> new ArrayList<Object>(columnarData.size()))
            .collect(Collectors.toList());
    for (List<Object> column : columnarData) {
      for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
        rowBasedData.get(rowIndex).add(column.get(rowIndex));
      }
    }
    return rowBasedData;
  }

  public static List<List<Object>> extractValuesFromRowSet(TRowSet rowSet) {
    if (rowSet == null || rowSet.getColumns() == null) {
      return Collections.emptyList();
    }
    return rowSet.getColumns().stream()
        .map(DatabricksThriftHelper::getColumnValues)
        .map(list -> new ArrayList<Object>(list))
        .collect(Collectors.toUnmodifiableList());
  }

  public static int getRowCount(TRowSet resultData) {
    List<TColumn> columns = resultData.getColumns();
    if (columns == null || columns.isEmpty()) {
      return 0;
    }
    return getColumnValues(columns.get(0)).size();
  }
}
