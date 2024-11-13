package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/** Class for representation of Array complex object. */
public class DatabricksArray implements Array {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksArray.class);

  private final Object[] elements;
  private final String typeName;

  // Constants for type names
  private static final String TYPE_STRUCT = "STRUCT";
  private static final String TYPE_ARRAY = "ARRAY";
  private static final String TYPE_MAP = "MAP";
  private static final String TYPE_INT = "INT";
  private static final String TYPE_INTEGER = "INTEGER";
  private static final String TYPE_BIGINT = "BIGINT";
  private static final String TYPE_SMALLINT = "SMALLINT";
  private static final String TYPE_FLOAT = "FLOAT";
  private static final String TYPE_DOUBLE = "DOUBLE";
  private static final String TYPE_DECIMAL = "DECIMAL";
  private static final String TYPE_NUMERIC = "NUMERIC";
  private static final String TYPE_BOOLEAN = "BOOLEAN";
  private static final String TYPE_DATE = "DATE";
  private static final String TYPE_TIMESTAMP = "TIMESTAMP";
  private static final String TYPE_TIME = "TIME";
  private static final String TYPE_BINARY = "BINARY";
  private static final String TYPE_STRING = "STRING";
  private static final String TYPE_VARCHAR = "VARCHAR";
  private static final String TYPE_CHAR = "CHAR";

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
        if (elementType.startsWith(TYPE_STRUCT)) {
          LOGGER.trace("Processing STRUCT element at index {}", i);
          if (element instanceof Map) {
            convertedElements[i] = new DatabricksStruct((Map<String, Object>) element, elementType);
          } else if (element instanceof String) {
            ComplexDataTypeParser parser = new ComplexDataTypeParser();
            Map<String, Object> structMap = parser.parseToMap((String) element, elementType);
            convertedElements[i] = new DatabricksStruct(structMap, elementType);
          } else {
            throw new IllegalArgumentException(
                "Expected a Map or String for STRUCT but found: "
                    + element.getClass().getSimpleName());
          }
        } else if (elementType.startsWith(TYPE_ARRAY)) {
          LOGGER.trace("Processing ARRAY element at index {}", i);
          if (element instanceof List) {
            convertedElements[i] = new DatabricksArray((List<Object>) element, elementType);
          } else if (element instanceof String) {
            ComplexDataTypeParser parser = new ComplexDataTypeParser();
            List<Object> arrayList =
                parser.parseToArray(parser.parse((String) element), elementType);
            convertedElements[i] = new DatabricksArray(arrayList, elementType);
          } else {
            throw new IllegalArgumentException(
                "Expected a List or String for ARRAY but found: "
                    + element.getClass().getSimpleName());
          }
        } else if (elementType.startsWith(TYPE_MAP)) {
          LOGGER.trace("Processing MAP element at index {}", i);
          if (element instanceof Map) {
            convertedElements[i] = new DatabricksMap<>((Map<String, Object>) element, elementType);
          } else if (element instanceof String) {
            ComplexDataTypeParser parser = new ComplexDataTypeParser();
            Map<String, Object> map = parser.parseToMap((String) element, elementType);
            convertedElements[i] = new DatabricksMap<>(map, elementType);
          } else {
            throw new IllegalArgumentException(
                "Expected a Map or String for MAP but found: "
                    + element.getClass().getSimpleName());
          }
        } else {
          convertedElements[i] = convertValue(element, elementType);
        }
      } catch (Exception e) {
        LOGGER.error("Error converting element at index {}: {}", i, e.getMessage(), e);
        throw new IllegalArgumentException("Error converting elements", e);
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

    try {
      switch (type.toUpperCase()) {
        case TYPE_INT:
        case TYPE_INTEGER:
          return Integer.parseInt(value.toString());
        case TYPE_BIGINT:
          return Long.parseLong(value.toString());
        case TYPE_SMALLINT:
          return Short.parseShort(value.toString());
        case TYPE_FLOAT:
          return Float.parseFloat(value.toString());
        case TYPE_DOUBLE:
          return Double.parseDouble(value.toString());
        case TYPE_DECIMAL:
        case TYPE_NUMERIC:
          return new BigDecimal(value.toString());
        case TYPE_BOOLEAN:
          return Boolean.parseBoolean(value.toString());
        case TYPE_DATE:
          return Date.valueOf(value.toString());
        case TYPE_TIMESTAMP:
          return Timestamp.valueOf(value.toString());
        case TYPE_TIME:
          return Time.valueOf(value.toString());
        case TYPE_BINARY:
          return value instanceof byte[] ? value : value.toString().getBytes();
        case TYPE_STRING:
        case TYPE_VARCHAR:
        case TYPE_CHAR:
        default:
          return value.toString();
      }
    } catch (Exception e) {
      LOGGER.error("Error converting simple value of type {}: {}", type, e.getMessage(), e);
      throw new IllegalArgumentException(
          "Failed to convert value " + value + " to type " + type, e);
    }
  }

  /**
   * Retrieves the base SQL type name of the array.
   *
   * @return the SQL type name of the array elements
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getBaseTypeName() throws SQLException {
    LOGGER.debug("Getting base type name");
    return this.typeName;
  }

  /**
   * Retrieves the base SQL type of the array.
   *
   * @return the base SQL type of the array elements
   * @throws SQLException if a database access error occurs
   */
  @Override
  public int getBaseType() throws SQLException {
    LOGGER.debug("Getting base type");
    return java.sql.Types.OTHER; // Or appropriate SQL type
  }

  /**
   * Retrieves the array elements as an Object array.
   *
   * @return the array elements
   * @throws SQLException if a database access error occurs
   */
  @Override
  public Object getArray() throws SQLException {
    LOGGER.debug("Getting array elements");
    return this.elements;
  }

  /**
   * Retrieves the array elements as an Object array with a specified type map.
   *
   * @param map a Map object that contains the mapping of SQL types to Java classes
   * @return the array elements
   * @throws SQLException if a database access error occurs
   */
  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("Getting array with type map");
    return this.getArray();
  }

  /**
   * Retrieves a portion of the array elements as an Object array.
   *
   * @param index the index of the first element to retrieve
   * @param count the number of elements to retrieve
   * @return the specified portion of the array elements
   * @throws SQLException if a database access error occurs
   */
  @Override
  public Object getArray(long index, int count) throws SQLException {
    LOGGER.debug("Getting subarray from index {} with count {}", index, count);
    return java.util.Arrays.copyOfRange(this.elements, (int) index - 1, (int) index - 1 + count);
  }

  /**
   * Retrieves a portion of the array elements as an Object array with a specified type map.
   *
   * @param index the index of the first element to retrieve
   * @param count the number of elements to retrieve
   * @param map a Map object that contains the mapping of SQL types to Java classes
   * @return the specified portion of the array elements
   * @throws SQLException if a database access error occurs
   */
  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("Getting subarray with type map from index {} with count {}", index, count);
    return this.getArray(index, count);
  }

  /**
   * Frees any resources held by this array object. This implementation does not hold resources.
   *
   * @throws SQLException if a database access error occurs
   */
  @Override
  public void free() throws SQLException {
    LOGGER.debug("Freeing resources (if any)");
  }

  /**
   * Retrieves the array as a ResultSet object. Not implemented in this class.
   *
   * @return nothing, as this method is not implemented
   * @throws SQLException always thrown as this method is not supported
   */
  @Override
  public java.sql.ResultSet getResultSet() throws SQLException {
    LOGGER.error("getResultSet() not implemented");
    throw new UnsupportedOperationException("getResultSet() not implemented");
  }

  /**
   * Retrieves the array as a ResultSet object with a specified type map. Not implemented in this
   * class.
   *
   * @param map a Map object that contains the mapping of SQL types to Java classes
   * @return nothing, as this method is not implemented
   * @throws SQLException always thrown as this method is not supported
   */
  @Override
  public java.sql.ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    LOGGER.error("getResultSet(Map<String, Class<?>> map) not implemented");
    throw new UnsupportedOperationException(
        "getResultSet(Map<String, Class<?>> map) not implemented");
  }

  /**
   * Retrieves a portion of the array as a ResultSet object. Not implemented in this class.
   *
   * @param index the index of the first element to retrieve
   * @param count the number of elements to retrieve
   * @return nothing, as this method is not implemented
   * @throws SQLException always thrown as this method is not supported
   */
  @Override
  public java.sql.ResultSet getResultSet(long index, int count) throws SQLException {
    LOGGER.error("getResultSet(long index, int count) not implemented");
    throw new UnsupportedOperationException("getResultSet(long index, int count) not implemented");
  }

  /**
   * Retrieves a portion of the array as a ResultSet object with a specified type map. Not
   * implemented in this class.
   *
   * @param index the index of the first element to retrieve
   * @param count the number of elements to retrieve
   * @param map a Map object that contains the mapping of SQL types to Java classes
   * @return nothing, as this method is not implemented
   * @throws SQLException always thrown as this method is not supported
   */
  @Override
  public java.sql.ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    LOGGER.error("getResultSet(long index, int count, Map<String, Class<?>> map) not implemented");
    throw new UnsupportedOperationException(
        "getResultSet(long index, int count, Map<String, Class<?>> map) not implemented");
  }
}
