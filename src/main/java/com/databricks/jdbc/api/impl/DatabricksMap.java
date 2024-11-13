package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Class for representation of Map complex object. */
public class DatabricksMap<K, V> implements Map<K, V> {
  private final Map<K, V> map;
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksMap.class);

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
   * Constructs a DatabricksMap with the specified map and metadata.
   *
   * @param map the original map to be converted
   * @param metadata the metadata for type conversion
   */
  public DatabricksMap(Map<K, V> map, String metadata) {
    LOGGER.debug("Initializing DatabricksMap with metadata: {}", metadata);
    this.map = convertMap(map, metadata);
  }

  /**
   * Converts the provided map according to specified metadata.
   *
   * @param originalMap the original map to be converted
   * @param metadata the metadata for type conversion
   * @return a converted map
   */
  private Map<K, V> convertMap(Map<K, V> originalMap, String metadata) {
    LOGGER.debug("Converting map with metadata: {}", metadata);
    Map<K, V> convertedMap = new LinkedHashMap<>();
    try {
      String[] mapMetadata = MetadataParser.parseMapMetadata(metadata).split(",", 2);
      String keyType = mapMetadata[0].trim();
      String valueType = mapMetadata[1].trim();
      LOGGER.debug("Parsed metadata - Key Type: {}, Value Type: {}", keyType, valueType);

      for (Map.Entry<K, V> entry : originalMap.entrySet()) {
        K key = entry.getKey();
        V value = convertValue(entry.getValue(), valueType);
        convertedMap.put(key, value);
        LOGGER.trace("Converted entry - Key: {}, Converted Value: {}", key, value);
      }
    } catch (Exception e) {
      LOGGER.error("Error during map conversion: {}", e.getMessage(), e);
      throw new IllegalArgumentException("Invalid metadata or map structure", e);
    }
    return convertedMap;
  }

  /**
   * Converts the value according to specified type.
   *
   * @param value the value to be converted
   * @param valueType the type to convert the value to
   * @return the converted value
   */
  private V convertValue(V value, String valueType) {
    try {
      LOGGER.debug("Converting value of type: {}", valueType);
      if (valueType.startsWith(TYPE_STRUCT)) {
        if (value instanceof Map) {
          LOGGER.trace("Converting value as STRUCT");
          return (V) new DatabricksStruct((Map<String, Object>) value, valueType);
        } else {
          LOGGER.error("Expected a Map for STRUCT but found: {}", value.getClass().getSimpleName());
          throw new IllegalArgumentException(
              "Expected a Map for STRUCT but found: " + value.getClass().getSimpleName());
        }
      } else if (valueType.startsWith(TYPE_ARRAY)) {
        if (value instanceof List) {
          LOGGER.trace("Converting value as ARRAY");
          return (V) new DatabricksArray((List<Object>) value, valueType);
        } else {
          LOGGER.error("Expected a List for ARRAY but found: {}", value.getClass().getSimpleName());
          throw new IllegalArgumentException(
              "Expected a List for ARRAY but found: " + value.getClass().getSimpleName());
        }
      } else if (valueType.startsWith(TYPE_MAP)) {
        if (value instanceof Map) {
          LOGGER.trace("Converting value as MAP");
          return (V) new DatabricksMap<>((Map<String, Object>) value, valueType);
        } else {
          LOGGER.error("Expected a Map for MAP but found: {}", value.getClass().getSimpleName());
          throw new IllegalArgumentException(
              "Expected a Map for MAP but found: " + value.getClass().getSimpleName());
        }
      } else {
        return convertSimpleValue(value, valueType);
      }
    } catch (Exception e) {
      LOGGER.error("Error converting value of type {}: {}", valueType, e.getMessage(), e);
      throw e;
    }
  }

  /**
   * Converts a simple value to the specified type.
   *
   * @param value the value to be converted
   * @param valueType the type to convert the value to
   * @return the converted simple value
   */
  private V convertSimpleValue(V value, String valueType) {
    LOGGER.trace("Converting simple value of type: {}", valueType);
    if (value == null) {
      return null;
    }

    try {
      switch (valueType.toUpperCase()) {
        case TYPE_INT:
        case TYPE_INTEGER:
          return (V) Integer.valueOf(value.toString());
        case TYPE_BIGINT:
          return (V) Long.valueOf(value.toString());
        case TYPE_SMALLINT:
          return (V) Short.valueOf(value.toString());
        case TYPE_FLOAT:
          return (V) Float.valueOf(value.toString());
        case TYPE_DOUBLE:
          return (V) Double.valueOf(value.toString());
        case TYPE_DECIMAL:
        case TYPE_NUMERIC:
          return (V) new BigDecimal(value.toString());
        case TYPE_BOOLEAN:
          return (V) Boolean.valueOf(value.toString());
        case TYPE_DATE:
          return (V) Date.valueOf(value.toString());
        case TYPE_TIMESTAMP:
          return (V) Timestamp.valueOf(value.toString());
        case TYPE_TIME:
          return (V) Time.valueOf(value.toString());
        case TYPE_BINARY:
          return (V) (value instanceof byte[] ? value : value.toString().getBytes());
        case TYPE_STRING:
        case TYPE_VARCHAR:
        case TYPE_CHAR:
        default:
          return (V) value.toString();
      }
    } catch (Exception e) {
      LOGGER.error("Error converting simple value of type {}: {}", valueType, e.getMessage(), e);
      throw new IllegalArgumentException(
          "Failed to convert value " + value + " to type " + valueType, e);
    }
  }

  /**
   * @return the size of the map
   */
  @Override
  public int size() {
    LOGGER.trace("Getting map size");
    return map.size();
  }

  /**
   * @return true if the map is empty, otherwise false
   */
  @Override
  public boolean isEmpty() {
    LOGGER.trace("Checking if map is empty");
    return map.isEmpty();
  }

  /**
   * Checks if the map contains the specified key.
   *
   * @param key the key to check for
   * @return true if the map contains the specified key
   */
  @Override
  public boolean containsKey(Object key) {
    LOGGER.trace("Checking if map contains key: {}", key);
    return map.containsKey(key);
  }

  /**
   * Checks if the map contains the specified value.
   *
   * @param value the value to check for
   * @return true if the map contains the specified value
   */
  @Override
  public boolean containsValue(Object value) {
    LOGGER.trace("Checking if map contains value: {}", value);
    return map.containsValue(value);
  }

  /**
   * Retrieves the value associated with the specified key.
   *
   * @param key the key of the value to retrieve
   * @return the value associated with the key, or null if the key is not found
   */
  @Override
  public V get(Object key) {
    LOGGER.trace("Getting value for key: {}", key);
    return map.get(key);
  }

  /**
   * Associates the specified value with the specified key in the map.
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with key, or null if there was no mapping for key
   */
  @Override
  public V put(K key, V value) {
    LOGGER.debug("Putting key: {}, value: {} in map", key, value);
    return map.put(key, value);
  }

  /**
   * Removes the mapping for a key from this map if it is present.
   *
   * @param key key whose mapping is to be removed from the map
   * @return the previous value associated with key, or null if there was no mapping for key
   */
  @Override
  public V remove(Object key) {
    LOGGER.debug("Removing key: {} from map", key);
    return map.remove(key);
  }

  /**
   * Copies all of the mappings from the specified map to this map.
   *
   * @param m mappings to be stored in this map
   */
  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    LOGGER.debug("Putting all entries from given map into current map");
    map.putAll(m);
  }

  /** Clears the map of all entries. */
  @Override
  public void clear() {
    LOGGER.debug("Clearing map");
    map.clear();
  }

  /**
   * @return a set view of the keys contained in this map
   */
  @Override
  public java.util.Set<K> keySet() {
    LOGGER.trace("Getting key set from map");
    return map.keySet();
  }

  /**
   * @return a collection view of the values contained in this map
   */
  @Override
  public java.util.Collection<V> values() {
    LOGGER.trace("Getting values collection from map");
    return map.values();
  }

  /**
   * @return a set view of the mappings contained in this map
   */
  @Override
  public java.util.Set<Entry<K, V>> entrySet() {
    LOGGER.trace("Getting entry set from map");
    return map.entrySet();
  }
}
