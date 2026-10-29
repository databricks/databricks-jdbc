package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.IDatabricksGeospatial;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ColumnMetadata;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.service.sql.StatementState;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Class for representation of Array complex object. */
public class DatabricksArray implements Array {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksArray.class);

  private final Object[] elements;
  private final String typeName;
  private final String elementType;

  /**
   * Constructs a DatabricksArray with the specified elements and metadata.
   *
   * @param elements the elements of the array as a list
   * @param metadata the metadata describing the type of array elements
   */
  public DatabricksArray(List<Object> elements, String metadata) {
    LOGGER.debug("Initializing DatabricksArray with metadata: {}", metadata);
    String elementType = MetadataParser.parseArrayMetadata(metadata);
    this.elements = convertElements(elements, elementType);
    this.typeName = metadata;
    this.elementType = elementType;
  }

  /**
   * Converts the elements based on specified element type.
   *
   * @param elements the original elements to be converted
   * @param elementType the type of each element
   * @return an array of converted elements
   */
  private Object[] convertElements(List<Object> elements, String elementType) {
    LOGGER.debug("Converting elements with element type: {}", elementType);
    Object[] convertedElements = new Object[elements.size()];

    for (int i = 0; i < elements.size(); i++) {
      Object element = elements.get(i);
      try {
        if (elementType.startsWith(DatabricksTypeUtil.STRUCT)) {
          if (element instanceof Map) {
            convertedElements[i] = new DatabricksStruct((Map<String, Object>) element, elementType);
          } else if (element instanceof DatabricksStruct) {
            convertedElements[i] = element;
          } else if (element == null) {
            convertedElements[i] = null;
          } else {
            throw new DatabricksDriverException(
                "Expected a Map for STRUCT but found: " + element.getClass().getSimpleName(),
                DatabricksDriverErrorCode.COMPLEX_DATA_TYPE_ARRAY_CONVERSION_ERROR);
          }
        } else if (elementType.startsWith(DatabricksTypeUtil.ARRAY)) {
          if (element instanceof List) {
            convertedElements[i] = new DatabricksArray((List<Object>) element, elementType);
          } else if (element instanceof DatabricksArray) {
            convertedElements[i] = element;
          } else if (element == null) {
            convertedElements[i] = null;
          } else {
            throw new DatabricksDriverException(
                "Expected a List for ARRAY but found: " + element.getClass().getSimpleName(),
                DatabricksDriverErrorCode.COMPLEX_DATA_TYPE_ARRAY_CONVERSION_ERROR);
          }
        } else if (elementType.startsWith(DatabricksTypeUtil.MAP)) {
          if (element instanceof Map) {
            convertedElements[i] = new DatabricksMap<>((Map<String, Object>) element, elementType);
          } else if (element instanceof DatabricksMap) {
            convertedElements[i] = element;
          } else if (element == null) {
            convertedElements[i] = null;
          } else {
            throw new DatabricksDriverException(
                "Expected a Map for MAP but found: " + element.getClass().getSimpleName(),
                DatabricksDriverErrorCode.COMPLEX_DATA_TYPE_ARRAY_CONVERSION_ERROR);
          }
        } else {
          convertedElements[i] = convertValue(element, elementType);
        }
      } catch (Exception e) {
        String errorMessage =
            String.format("Error converting element at index %s: %s", i, e.getMessage());
        LOGGER.error(e, errorMessage);
        throw new DatabricksDriverException(
            "Error converting elements",
            e,
            DatabricksDriverErrorCode.COMPLEX_DATA_TYPE_ARRAY_CONVERSION_ERROR);
      }
    }
    return convertedElements;
  }

  /**
   * Converts a simple element to the specified type.
   *
   * @param value the value to convert
   * @param type the type to convert the value to
   * @return the converted value
   */
  private Object convertValue(Object value, String type) {
    LOGGER.trace("Converting simple value of type: {}", type);
    if (value == null) {
      return null;
    }
    if (DatabricksTypeUtil.isGeospatialType(type.toUpperCase(Locale.ROOT))) {
      return value;
    }

    try {
      switch (type.toUpperCase()) {
        case DatabricksTypeUtil.INT:
          return Integer.parseInt(value.toString());
        case DatabricksTypeUtil.BIGINT:
          return Long.parseLong(value.toString());
        case DatabricksTypeUtil.SMALLINT:
          return Short.parseShort(value.toString());
        case DatabricksTypeUtil.FLOAT:
          return Float.parseFloat(value.toString());
        case DatabricksTypeUtil.DOUBLE:
          return Double.parseDouble(value.toString());
        case DatabricksTypeUtil.DECIMAL:
          return new BigDecimal(value.toString());
        case DatabricksTypeUtil.BOOLEAN:
          return Boolean.parseBoolean(value.toString());
        case DatabricksTypeUtil.DATE:
          return Date.valueOf(value.toString());
        case DatabricksTypeUtil.TIMESTAMP:
        case DatabricksTypeUtil.TIMESTAMP_NTZ:
          return value instanceof Timestamp ? value : Timestamp.valueOf(value.toString());
        case DatabricksTypeUtil.TIME:
          return Time.valueOf(value.toString());
        case DatabricksTypeUtil.BINARY:
          return value instanceof byte[] ? value : value.toString().getBytes();
        case DatabricksTypeUtil.STRING:
        default:
          return value.toString();
      }
    } catch (Exception e) {
      String errorMessage =
          String.format("Error converting simple value of type %s: %s", type, e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksDriverException(
          errorMessage, DatabricksDriverErrorCode.COMPLEX_DATA_TYPE_ARRAY_CONVERSION_ERROR);
    }
  }

  @Override
  public String getBaseTypeName() throws SQLException {
    LOGGER.debug("Getting base type name");
    return this.typeName;
  }

  @Override
  public int getBaseType() throws SQLException {
    LOGGER.debug("Getting base type");
    return java.sql.Types.OTHER; // Or appropriate SQL type
  }

  @Override
  public Object getArray() throws SQLException {
    LOGGER.debug("Getting array elements");
    return this.elements;
  }

  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("Getting array with type map");
    return this.getArray();
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    LOGGER.debug("Getting subarray from index {} with count {}", index, count);
    return java.util.Arrays.copyOfRange(this.elements, (int) index - 1, (int) index - 1 + count);
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("Getting subarray with type map from index {} with count {}", index, count);
    return this.getArray(index, count);
  }

  @Override
  public void free() throws SQLException {
    LOGGER.debug("Freeing resources (if any)");
  }

  @Override
  public java.sql.ResultSet getResultSet() throws SQLException {
    LOGGER.debug("Getting result set for array");
    return getResultSet(1, this.elements.length);
  }

  @Override
  public java.sql.ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("Getting result set with type map for array");
    return getResultSet();
  }

  @Override
  public java.sql.ResultSet getResultSet(long index, int count) throws SQLException {
    LOGGER.debug("Getting result set from index {} with count {}", index, count);
    return buildResultSet(index, count);
  }

  @Override
  public java.sql.ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    LOGGER.debug("Getting result set with type map from index {} with count {}", index, count);
    return getResultSet(index, count);
  }

  /**
   * Builds a {@link java.sql.ResultSet} containing one row for each requested array element. Per
   * the JDBC {@link Array#getResultSet()} contract, column 1 holds the 1-based index of the element
   * within the array and column 2 holds the element value.
   *
   * @param index the 1-based index of the first element to return
   * @param count the number of elements to return
   * @return a result set over the requested window of elements
   */
  private java.sql.ResultSet buildResultSet(long index, int count) throws SQLException {
    if (index < 1) {
      throw new DatabricksSQLException(
          "Array index must be >= 1 but was " + index,
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR.name(),
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }
    if (count < 0) {
      throw new DatabricksSQLException(
          "Array element count must be >= 0 but was " + count,
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR.name(),
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }
    // Do the window arithmetic in long and clamp to the array bounds before narrowing, so that a
    // large index or a count such as Integer.MAX_VALUE cannot overflow.
    long startL = Math.min(index - 1, (long) this.elements.length);
    int start = (int) startL;
    int end = (int) Math.min((long) this.elements.length, startL + (long) count);

    ColumnInfoTypeName valueTypeName =
        DatabricksTypeUtil.getColumnInfoType(DatabricksTypeUtil.baseTypeName(this.elementType));
    if (valueTypeName == ColumnInfoTypeName.USER_DEFINED_TYPE) {
      // A collated element type (e.g. "STRING COLLATE UTF8_LCASE") does not map directly. Recover
      // STRING, as the other metadata paths do, so the VALUE column reports VARCHAR, not OTHER.
      ColumnInfoTypeName recovered = DatabricksTypeUtil.recoverStringType(this.elementType);
      if (recovered != null) {
        valueTypeName = recovered;
      }
    }
    String valueTypeText = this.elementType;
    if ((valueTypeName == ColumnInfoTypeName.GEOMETRY
            || valueTypeName == ColumnInfoTypeName.GEOGRAPHY)
        && hasStringFallbackGeospatialElement()) {
      // Match string fallbacks when geospatial support was disabled. Empty/all-null arrays retain
      // the declared type because DatabricksArray does not retain the connection's setting.
      valueTypeName = ColumnInfoTypeName.STRING;
      valueTypeText = DatabricksTypeUtil.STRING;
    }
    int valueType = DatabricksTypeUtil.getColumnType(valueTypeName);
    int[] valuePrecisionAndScale = precisionAndScale(this.elementType, valueType);

    List<ColumnMetadata> columnMetadataList = new ArrayList<>();
    columnMetadataList.add(
        new ColumnMetadata.Builder()
            .name("INDEX")
            .typeText(DatabricksTypeUtil.BIGINT)
            .typeInt(Types.BIGINT)
            .precision(DatabricksTypeUtil.getPrecision(Types.BIGINT))
            .scale(DatabricksTypeUtil.getScale(Types.BIGINT))
            .nullable(ResultSetMetaData.columnNoNulls)
            .build());
    columnMetadataList.add(
        new ColumnMetadata.Builder()
            .name("VALUE")
            // Keep the full element type text (e.g. ARRAY<INT>, STRUCT<a:INT>, DECIMAL(10,2))
            // rather than the base name: getObject() needs the parameterized text to parse nested
            // complex values, and it tells callers the exact element type. As a result,
            // getColumnTypeName(2) may differ from the base name used by query result metadata.
            // DatabricksArray has no connection context, so the EnableTimestampNtzTypeName
            // setting is not applied here: an ARRAY<TIMESTAMP_NTZ> always reports TIMESTAMP_NTZ,
            // even when the connection would normalize that name to TIMESTAMP elsewhere.
            .typeText(valueTypeText)
            .typeInt(valueType)
            .precision(valuePrecisionAndScale[0])
            .scale(valuePrecisionAndScale[1])
            .nullable(ResultSetMetaData.columnNullable)
            .build());

    List<List<Object>> rows = new ArrayList<>();
    for (int i = start; i < end; i++) {
      List<Object> row = new ArrayList<>();
      row.add((long) (i + 1));
      row.add(this.elements[i]);
      rows.add(row);
    }

    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        new StatementId("array-result-set"),
        columnMetadataList,
        rows,
        StatementType.QUERY,
        // A DatabricksArray only exists when complex datatype support is enabled, so nested
        // ARRAY/STRUCT/MAP elements must be returned as their Databricks types.
        /* complexDatatypeSupport= */ true,
        // Column types come from the declared (parameterized) element typeText.
        /* useTypeTextForColumnType= */ true);
  }

  /**
   * Returns whether any non-null element of a geospatial array holds a string fallback rather than
   * a native {@link IDatabricksGeospatial} value, i.e. geospatial support was disabled when the
   * array was parsed.
   */
  private boolean hasStringFallbackGeospatialElement() {
    for (Object element : this.elements) {
      if (element != null && !(element instanceof IDatabricksGeospatial)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns {precision, scale} for the given element type. For {@code DECIMAL(p,s)} (or {@code
   * DECIMAL(p)}) the declared parameters are used; otherwise the per-SQL-type defaults apply.
   *
   * <p>This is intentionally separate from {@code
   * DatabricksResultSetMetaData#getPrecisionAndScale(String, int)}. That regex only matches {@code
   * DECIMAL(p,s)}. This one also accepts {@code DECIMAL(p)} (scale 0, as in Spark SQL) because
   * array element type text can be written that way. The existing DESCRIBE QUERY path is left
   * unchanged in this fix. Merging the two parsers is left for a separate refactor.
   */
  private static int[] precisionAndScale(String typeText, int sqlType) {
    int precision = DatabricksTypeUtil.getPrecision(sqlType);
    int scale = DatabricksTypeUtil.getScale(sqlType);
    if (sqlType == Types.DECIMAL && typeText != null) {
      String normalized = typeText.trim();
      int open = normalized.indexOf('(');
      int close = normalized.indexOf(')', open + 1);
      if (open >= 0 && close > open) {
        String[] parts = normalized.substring(open + 1, close).split(",");
        try {
          precision = Integer.parseInt(parts[0].trim());
          scale = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
        } catch (NumberFormatException e) {
          LOGGER.debug("Could not parse precision/scale from element type {}", typeText);
          precision = DatabricksTypeUtil.getPrecision(sqlType);
          scale = DatabricksTypeUtil.getScale(sqlType);
        }
      }
    }
    return new int[] {precision, scale};
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < elements.length; i++) {
      if (i > 0) {
        sb.append(",");
      }

      sb.append(JsonUtil.formatComplexValue(elements[i]));
    }
    sb.append("]");
    return sb.toString();
  }
}
