package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.util.DatabricksTypeUtil;
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
        K key = convertSimpleValue(entry.getKey(), keyType);
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
   * Converts the value according to the specified type.
   *
   * @param value the value to be converted
   * @param valueType the type to convert the value to
   * @return the converted value
   */
  private V convertValue(V value, String valueType) {
    try {
      LOGGER.debug("Converting value of type: {}", valueType);
      if (valueType.startsWith(DatabricksTypeUtil.STRUCT)) {
        if (value instanceof Map) {
          LOGGER.trace("Converting value as STRUCT");
          return (V) new DatabricksStruct((Map<String, Object>) value, valueType);
        } else if (value instanceof DatabricksStruct) {
          return (V) value;
        } else {
          throw new IllegalArgumentException(
              "Expected a Map for STRUCT but found: " + value.getClass().getSimpleName());
        }
      } else if (valueType.startsWith(DatabricksTypeUtil.ARRAY)) {
        if (value instanceof List) {
          LOGGER.trace("Converting value as ARRAY");
          return (V) new DatabricksArray((List<Object>) value, valueType);
        } else if (value instanceof DatabricksArray) {
          return (V) value;
        } else {
          throw new IllegalArgumentException(
              "Expected a List for ARRAY but found: " + value.getClass().getSimpleName());
        }
      } else if (valueType.startsWith(DatabricksTypeUtil.MAP)) {
        if (value instanceof Map) {
          LOGGER.trace("Converting value as MAP");
          return (V) new DatabricksMap<>((Map<String, Object>) value, valueType);
        } else if (value instanceof DatabricksMap) {
          return (V) value;
        } else {
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
   * @return the converted value
   */
  @SuppressWarnings("unchecked")
  private <T> T convertSimpleValue(Object value, String valueType) {
    if (value == null) {
      return null;
    }

    try {
      switch (valueType.toUpperCase()) {
        case DatabricksTypeUtil.INT:
          return (T) Integer.valueOf(value.toString());
        case DatabricksTypeUtil.BIGINT:
          return (T) Long.valueOf(value.toString());
        case DatabricksTypeUtil.SMALLINT:
          return (T) Short.valueOf(value.toString());
        case DatabricksTypeUtil.FLOAT:
          return (T) Float.valueOf(value.toString());
        case DatabricksTypeUtil.DOUBLE:
          return (T) Double.valueOf(value.toString());
        case DatabricksTypeUtil.DECIMAL:
          return (T) new BigDecimal(value.toString());
        case DatabricksTypeUtil.BOOLEAN:
          return (T) Boolean.valueOf(value.toString());
        case DatabricksTypeUtil.DATE:
          return (T) Date.valueOf(value.toString());
        case DatabricksTypeUtil.TIMESTAMP:
          return (T) Timestamp.valueOf(value.toString());
        case DatabricksTypeUtil.TIME:
          return (T) Time.valueOf(value.toString());
        case DatabricksTypeUtil.BINARY:
          return (T) (value instanceof byte[] ? value : value.toString().getBytes());
        case DatabricksTypeUtil.STRING:
        default:
          return (T) value.toString();
      }
    } catch (Exception e) {
      LOGGER.error("Error converting simple value of type {}: {}", valueType, e.getMessage(), e);
      throw new IllegalArgumentException(
          "Failed to convert value " + value + " to type " + valueType, e);
    }
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return map.get(key);
  }

  @Override
  public V put(K key, V value) {
    return map.put(key, value);
  }

  @Override
  public V remove(Object key) {
    return map.remove(key);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    map.putAll(m);
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public java.util.Set<K> keySet() {
    return map.keySet();
  }

  @Override
  public java.util.Collection<V> values() {
    return map.values();
  }

  @Override
  public java.util.Set<Entry<K, V>> entrySet() {
    return map.entrySet();
  }

  @Override
  public String toString() {
    return map.toString();
  }
}
