package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.impl.converters.JavaTimeConverter;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Class for representation of Array complex object. */
public class DatabricksArray implements Array {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksArray.class);
  private static final Pattern DECIMAL_TYPE =
      Pattern.compile("DECIMAL\\((\\d+),\\s*(\\d+)\\)", Pattern.CASE_INSENSITIVE);

  private final Object[] elements;
  private final String elementType;
  // Sources of the exact java.time values of DATE/TIMESTAMP elements (JSON leaves, converted on
  // demand, or java.time values), parallel to elements; null when unknown.
  private final Object[] temporalElements;
  // Session time zone of nested Arrow TIMESTAMP values, for java.time results; null if unknown.
  private final ZoneId timestampZone;
  // Set once an element fell back to its java.sql value, so the warning is logged once.
  private boolean warnedInexact;

  /**
   * Constructs a DatabricksArray with the specified elements and metadata.
   *
   * @param elements the elements of the array as a list
   * @param metadata the metadata describing the type of array elements
   */
  public DatabricksArray(List<Object> elements, String metadata) {
    this(elements, metadata, null, null);
  }

  /**
   * Constructs a DatabricksArray that also keeps the exact {@code java.time} values of DATE and
   * TIMESTAMP elements. {@link #getArray()} still returns {@code java.sql} values; the exact values
   * are returned by {@link #getArray(Map)} with a {@code java.time} mapping and by {@code
   * getResultSet().getObject(2, java.time class)}.
   *
   * @param elements the elements of the array as a list
   * @param metadata the metadata describing the type of array elements
   * @param temporalElements sources of the exact values parallel to {@code elements} (JSON leaves,
   *     converted only when requested, or {@code java.time} values), may be null; null entries fall
   *     back to the {@code java.sql} element
   * @param timestampZone the session time zone of nested Arrow TIMESTAMP values, in which their
   *     {@code java.time} values are reported, or null if unknown
   */
  DatabricksArray(
      List<Object> elements, String metadata, List<Object> temporalElements, ZoneId timestampZone) {
    LOGGER.debug("Initializing DatabricksArray with metadata: {}", metadata);
    String elementType = MetadataParser.parseArrayMetadata(metadata);
    this.elements = convertElements(elements, elementType);
    this.elementType = elementType;
    this.temporalElements =
        temporalElements != null && temporalElements.size() == elements.size()
            ? temporalElements.toArray()
            : null;
    this.timestampZone = timestampZone;
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
          // Keep a parsed Date: rebuilding it from toString() drops the BC era.
          return value instanceof Date ? value : Date.valueOf(value.toString());
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

  /** Returns the SQL type name of the elements, e.g. {@code INT} for {@code ARRAY<INT>}. */
  @Override
  public String getBaseTypeName() throws SQLException {
    LOGGER.debug("Getting base type name");
    return this.elementType;
  }

  /**
   * Returns the {@link Types} code of the elements, e.g. {@code INTEGER} for {@code ARRAY<INT>}.
   */
  @Override
  public int getBaseType() throws SQLException {
    LOGGER.debug("Getting base type");
    return elementSqlType();
  }

  @Override
  public Object getArray() throws SQLException {
    LOGGER.debug("Getting array elements");
    return this.elements;
  }

  /**
   * Returns the elements. If {@code map} maps the element type (DATE, TIMESTAMP or TIMESTAMP_NTZ)
   * to a {@code java.time} class, for example {@code Map.of("DATE", LocalDate.class)}, the elements
   * are returned as that class, built from the exact values instead of the {@code java.sql} ones.
   * Other mappings are ignored.
   */
  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("Getting array with type map");
    if (JavaTimeConverter.getJavaTimeMapping(map, this.elementType) == null) {
      return this.getArray();
    }
    return this.getArray(1, this.elements.length, map);
  }

  /**
   * The exact {@code java.time} value of the element at 0-based {@code i}, or null if unknown. A
   * JSON leaf is parsed on the first call and replaced by its result, so later reads neither parse
   * it again nor repeat the warning for a value that has no exact form.
   *
   * <p>The cache is not synchronized. If threads share this array, a leaf may be parsed twice or
   * the warning logged twice, but every read returns the same value: each parse gives the same
   * result and the cached values are immutable.
   */
  private Object exactElement(int i) {
    if (this.temporalElements == null || i < 0 || i >= this.temporalElements.length) {
      return null;
    }
    Object source = this.temporalElements[i];
    Object exact = ComplexDataTypeParser.toExactTemporal(source, this.elementType);
    if (ComplexDataTypeParser.isUnparsedLeaf(source)) {
      if (exact == null && !this.warnedInexact) {
        this.warnedInexact = true;
        ComplexDataTypeParser.warnInexactNestedValue(this.elementType);
      }
      this.temporalElements[i] = exact;
    }
    return exact;
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    LOGGER.debug("Getting subarray from index {} with count {}", index, count);
    return java.util.Arrays.copyOfRange(this.elements, (int) index - 1, (int) index - 1 + count);
  }

  /** Returns a slice of the elements, applying {@code map} as {@link #getArray(Map)} does. */
  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("Getting subarray with type map from index {} with count {}", index, count);
    Class<?> javaTimeType = JavaTimeConverter.getJavaTimeMapping(map, this.elementType);
    Object[] slice = (Object[]) this.getArray(index, count);
    if (javaTimeType == null) {
      return slice;
    }
    Object[] converted = new Object[slice.length];
    for (int i = 0; i < slice.length; i++) {
      Object exact = exactElement((int) index - 1 + i);
      Object value =
          exact != null ? ComplexDataTypeParser.inTimestampZone(exact, timestampZone) : slice[i];
      converted[i] = JavaTimeConverter.convertNested(javaTimeType, this.elementType, value);
    }
    return converted;
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

  /**
   * Returns the elements as a result set. A {@code java.time} mapping for the element type, as in
   * {@link #getArray(Map)}, makes {@code getObject(2)} return that class; other mappings are
   * ignored.
   */
  @Override
  public java.sql.ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("Getting result set with type map for array");
    return getResultSet(1, this.elements.length, map);
  }

  @Override
  public java.sql.ResultSet getResultSet(long index, int count) throws SQLException {
    LOGGER.debug("Getting result set from index {} with count {}", index, count);
    return buildResultSet(index, count, null);
  }

  /** Returns a slice of the elements, applying {@code map} as {@link #getResultSet(Map)} does. */
  @Override
  public java.sql.ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    LOGGER.debug("Getting result set with type map from index {} with count {}", index, count);
    return buildResultSet(index, count, map);
  }

  /**
   * Builds a result set with one row per element in the requested window. Per the {@link
   * Array#getResultSet()} contract, column 1 is the 1-based element index and column 2 the value.
   */
  private java.sql.ResultSet buildResultSet(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
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
    // Clamp in long so a large index or count cannot overflow.
    int start = (int) Math.min(index - 1, this.elements.length);
    int end = (int) Math.min(this.elements.length, start + (long) count);

    int valueType = elementSqlType();
    int valuePrecision = DatabricksTypeUtil.getPrecision(valueType);
    int valueScale = DatabricksTypeUtil.getScale(valueType);
    // DECIMAL values are read at the column scale, so use the declared one.
    Matcher decimal = DECIMAL_TYPE.matcher(this.elementType.trim());
    if (decimal.matches()) {
      valuePrecision = Integer.parseInt(decimal.group(1));
      valueScale = Integer.parseInt(decimal.group(2));
    }

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
            // Full element type text, which getObject() uses to return nested complex values.
            .typeText(this.elementType)
            .typeInt(valueType)
            .precision(valuePrecision)
            .scale(valueScale)
            .nullable(ResultSetMetaData.columnNullable)
            .build());

    List<List<Object>> rows = new ArrayList<>();
    // Exact DATE/TIMESTAMP values for getObject(2, java.time class) and the numeric getters,
    // converted only when a cell is read.
    List<List<Object>> temporalRows = this.temporalElements == null ? null : new ArrayList<>();
    for (int i = start; i < end; i++) {
      List<Object> row = new ArrayList<>();
      row.add((long) (i + 1));
      row.add(this.elements[i]);
      rows.add(row);
      if (temporalRows != null) {
        int elementIndex = i;
        List<Object> temporalRow = new ArrayList<>();
        temporalRow.add(null);
        temporalRow.add((Supplier<Object>) () -> exactElement(elementIndex));
        temporalRows.add(temporalRow);
      }
    }

    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        new StatementId("array-result-set"),
        columnMetadataList,
        rows,
        temporalRows,
        map,
        timestampZone,
        StatementType.QUERY,
        // A DatabricksArray only exists when complex datatype support is enabled.
        /* complexDatatypeSupport= */ true);
  }

  // Uses the same mapping as ResultSetMetaData.getColumnType() for a column of the element type,
  // except MAP: that column mapping (VARCHAR) assumes complex support is off, but MAP elements are
  // always DatabricksMap objects.
  private int elementSqlType() {
    if (DatabricksTypeUtil.recoverStringType(this.elementType) != null) {
      return Types.VARCHAR; // collated strings, e.g. STRING COLLATE UTF8_LCASE
    }
    String baseType = baseTypeName(this.elementType);
    if (DatabricksTypeUtil.MAP.equals(baseType)) {
      return Types.OTHER;
    }
    return DatabricksTypeUtil.getColumnType(DatabricksTypeUtil.getColumnInfoType(baseType));
  }

  // Strips type parameters, e.g. DECIMAL(10,2) -> DECIMAL and ARRAY<INT> -> ARRAY.
  private static String baseTypeName(String typeText) {
    return typeText.split("[<(]", 2)[0].trim().toUpperCase(Locale.ROOT);
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
