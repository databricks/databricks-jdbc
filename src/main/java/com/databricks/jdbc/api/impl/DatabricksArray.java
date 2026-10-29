package com.databricks.jdbc.api.impl;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Class for representation of Array complex object. */
public class DatabricksArray implements Array {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksArray.class);
  private static final Pattern DECIMAL_TYPE =
      Pattern.compile("DECIMAL\\((\\d+),\\s*(\\d+)\\)", Pattern.CASE_INSENSITIVE);

  private final Object[] elements;
  private final String elementType;
  private final String elementTypeName;

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
    this.elementType = elementType;
    this.elementTypeName = normalizeLeadingTypeKeyword(elementType);
  }

  /**
   * Upper-cases only the leading type keyword (e.g. {@code int} becomes {@code INT}, {@code
   * array<int>} becomes {@code ARRAY<int>}) so {@link #getBaseTypeName()} is consistent across
   * transports. Any {@code <...>}/{@code (...)} parameters are kept exactly as the server sent them
   * so struct field names keep their case.
   */
  private static String normalizeLeadingTypeKeyword(String typeName) {
    if (typeName == null) {
      return null;
    }
    String trimmed = typeName.trim();
    String keyword = stripTypeParameters(trimmed);
    return keyword.toUpperCase(Locale.ROOT) + trimmed.substring(keyword.length());
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
    // Dispatch case-insensitively (like getSqlTypeForElement) so getBaseType() always describes
    // the objects actually stored. The original elementType is still passed to nested complex
    // types so struct field names keep their case.
    String elementTypeUpper = elementType.trim().toUpperCase(Locale.ROOT);

    for (int i = 0; i < elements.size(); i++) {
      Object element = elements.get(i);
      try {
        if (elementTypeUpper.startsWith(DatabricksTypeUtil.STRUCT)) {
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
        } else if (elementTypeUpper.startsWith(DatabricksTypeUtil.ARRAY)) {
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
        } else if (elementTypeUpper.startsWith(DatabricksTypeUtil.MAP)) {
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
      // Switch on the base type name so parameterized types such as DECIMAL(10,2) convert to the
      // same Java type that getBaseType() reports.
      switch (stripTypeParameters(type.trim().toUpperCase(Locale.ROOT))) {
        // Aliases mirror DatabricksTypeUtil.getColumnInfoType so getBaseType() stays in sync.
        case DatabricksTypeUtil.INT:
        case DatabricksTypeUtil.INTEGER:
          return Integer.parseInt(value.toString());
        case DatabricksTypeUtil.BIGINT:
        case DatabricksTypeUtil.LONG:
          return Long.parseLong(value.toString());
        case DatabricksTypeUtil.SMALLINT:
        case DatabricksTypeUtil.SHORT:
          return Short.parseShort(value.toString());
        case DatabricksTypeUtil.TINYINT:
        case DatabricksTypeUtil.BYTE:
          return Byte.parseByte(value.toString());
        case DatabricksTypeUtil.FLOAT:
        case DatabricksTypeUtil.REAL:
          return Float.parseFloat(value.toString());
        case DatabricksTypeUtil.DOUBLE:
          return Double.parseDouble(value.toString());
        case DatabricksTypeUtil.DECIMAL:
        case DatabricksTypeUtil.NUMERIC:
        case DatabricksTypeUtil.DEC:
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

  /**
   * Returns the SQL type name of the elements in this array, as required by {@link
   * Array#getBaseTypeName()} (e.g. {@code INT} for {@code ARRAY<INT>}). The leading type keyword is
   * always upper case; any type parameters (e.g. the fields of a {@code STRUCT<...>}) keep the
   * casing of the server-provided metadata.
   */
  @Override
  public String getBaseTypeName() throws SQLException {
    LOGGER.debug("Getting base type name");
    return this.elementTypeName;
  }

  /**
   * Returns the JDBC type code ({@link java.sql.Types}) of the elements in this array, as required
   * by {@link Array#getBaseType()}.
   */
  @Override
  public int getBaseType() throws SQLException {
    LOGGER.debug("Getting base type");
    return getSqlTypeForElement(this.elementTypeName);
  }

  /**
   * Maps an element type name (possibly parameterized, e.g. {@code DECIMAL(10,2)} or {@code
   * STRUCT<a:INT>}) to its {@link java.sql.Types} code.
   */
  private static int getSqlTypeForElement(String elementTypeName) {
    if (elementTypeName == null || elementTypeName.trim().isEmpty()) {
      return java.sql.Types.OTHER;
    }
    String upper = elementTypeName.trim().toUpperCase(Locale.ROOT);
    if (DatabricksTypeUtil.recoverStringType(upper) != null) {
      return java.sql.Types.VARCHAR;
    }
    String baseName = stripTypeParameters(upper);
    // Report the type of the objects actually stored by convertElements/convertValue, which can
    // differ from the column-level ResultSetMetaData mapping: MAP elements are DatabricksMap
    // instances (not strings, so not VARCHAR), TIME elements are java.sql.Time values, and
    // FLOAT/REAL elements are java.lang.Float values (Types.REAL; Types.FLOAT maps to double).
    if (DatabricksTypeUtil.MAP.equals(baseName)) {
      return java.sql.Types.JAVA_OBJECT;
    }
    if (DatabricksTypeUtil.TIME.equals(baseName)) {
      return java.sql.Types.TIME;
    }
    if (DatabricksTypeUtil.FLOAT.equals(baseName) || DatabricksTypeUtil.REAL.equals(baseName)) {
      return java.sql.Types.REAL;
    }
    // VOID/NULL elements (e.g. the empty literal ARRAY(), typed ARRAY<VOID>) can only be null.
    if (DatabricksTypeUtil.VOID.equals(baseName) || DatabricksTypeUtil.NULL.equals(baseName)) {
      return java.sql.Types.NULL;
    }
    try {
      return DatabricksTypeUtil.getColumnType(DatabricksTypeUtil.getColumnInfoType(baseName));
    } catch (Exception e) {
      LOGGER.debug("Unable to map array element type {}: {}", elementTypeName, e.getMessage());
      return java.sql.Types.OTHER;
    }
  }

  /**
   * Returns the base name of a type, dropping any {@code (...)} or {@code <...>} parameters (e.g.
   * {@code DECIMAL(10,2)} becomes {@code DECIMAL}).
   */
  private static String stripTypeParameters(String typeName) {
    for (int i = 0; i < typeName.length(); i++) {
      char c = typeName.charAt(i);
      if (c == '<' || c == '(') {
        return typeName.substring(0, i).trim();
      }
    }
    return typeName.trim();
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
   * Builds a result set with one row per element in the requested window. Per the {@link
   * Array#getResultSet()} contract, column 1 is the 1-based element index and column 2 the value.
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
    // Clamp in long so a large index or count cannot overflow.
    int start = (int) Math.min(index - 1, this.elements.length);
    int end = (int) Math.min(this.elements.length, start + (long) count);

    int valueType =
        DatabricksTypeUtil.getColumnType(
            DatabricksTypeUtil.getColumnInfoType(baseTypeName(this.elementType)));
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
        // A DatabricksArray only exists when complex datatype support is enabled.
        /* complexDatatypeSupport= */ true);
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
