package com.databricks.jdbc.client.impl.thrift.commons;

import static com.databricks.jdbc.client.impl.helper.MetadataResultConstants.NULL_STRING;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
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
    if (column.isSetBinaryVal()) return column.getBinaryVal().getValues().get(0);
    if (column.isSetBoolVal()) return column.getBoolVal().getValues().get(0);
    if (column.isSetByteVal()) return column.getByteVal().getValues().get(0);
    if (column.isSetDoubleVal()) return column.getDoubleVal().getValues().get(0);
    if (column.isSetI16Val()) return column.getI16Val().getValues().get(0);
    if (column.isSetI32Val()) return column.getI32Val().getValues().get(0);
    if (column.isSetI64Val()) return column.getI64Val().getValues().get(0);
    return column.getStringVal().getValues().get(0); // Default case
  }

  public static int getRowCount(TRowSet resultData) {
    List<TColumn> columns = resultData.getColumns();
    if (columns == null || columns.isEmpty()) {
      return 0;
    }

    TColumn tColumn = columns.get(0);
    Function<TColumn, Integer> rowCountFunction =
        column -> {
          if (column.isSetBinaryVal()) return column.getBinaryVal().getValuesSize();
          if (column.isSetBoolVal()) return column.getBoolVal().getValuesSize();
          if (column.isSetByteVal()) return column.getByteVal().getValuesSize();
          if (column.isSetDoubleVal()) return column.getDoubleVal().getValuesSize();
          if (column.isSetI16Val()) return column.getI16Val().getValuesSize();
          if (column.isSetI32Val()) return column.getI32Val().getValuesSize();
          if (column.isSetI64Val()) return column.getI64Val().getValuesSize();
          return column.getStringVal().getValuesSize(); // Default case
        };

    return rowCountFunction.apply(tColumn);
  }
}
