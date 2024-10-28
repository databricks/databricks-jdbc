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

  public DatabricksMap(Map<K, V> map, String metadata) {
    LOGGER.debug("Initializing DatabricksMap with metadata: {}", metadata);
    this.map = convertMap(map, metadata);
  }

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

  private V convertValue(V value, String valueType) {
    try {
      LOGGER.debug("Converting value of type: {}", valueType);
      if (valueType.startsWith("STRUCT")) {
        if (value instanceof Map) {
          LOGGER.trace("Converting value as STRUCT");
          return (V) new DatabricksStruct((Map<String, Object>) value, valueType);
        } else {
          LOGGER.error("Expected a Map for STRUCT but found: {}", value.getClass().getSimpleName());
          throw new IllegalArgumentException(
              "Expected a Map for STRUCT but found: " + value.getClass().getSimpleName());
        }
      } else if (valueType.startsWith("ARRAY")) {
        if (value instanceof List) {
          LOGGER.trace("Converting value as ARRAY");
          return (V) new DatabricksArray((List<Object>) value, valueType);
        } else {
          LOGGER.error("Expected a List for ARRAY but found: {}", value.getClass().getSimpleName());
          throw new IllegalArgumentException(
              "Expected a List for ARRAY but found: " + value.getClass().getSimpleName());
        }
      } else if (valueType.startsWith("MAP")) {
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

  private V convertSimpleValue(V value, String valueType) {
    LOGGER.trace("Converting simple value of type: {}", valueType);
    if (value == null) {
      return null;
    }

    try {
      switch (valueType.toUpperCase()) {
        case "INT":
        case "INTEGER":
          return (V) Integer.valueOf(value.toString());
        case "BIGINT":
          return (V) Long.valueOf(value.toString());
        case "SMALLINT":
          return (V) Short.valueOf(value.toString());
        case "FLOAT":
          return (V) Float.valueOf(value.toString());
        case "DOUBLE":
          return (V) Double.valueOf(value.toString());
        case "DECIMAL":
        case "NUMERIC":
          return (V) new BigDecimal(value.toString());
        case "BOOLEAN":
          return (V) Boolean.valueOf(value.toString());
        case "DATE":
          return (V) Date.valueOf(value.toString());
        case "TIMESTAMP":
          return (V) Timestamp.valueOf(value.toString());
        case "TIME":
          return (V) Time.valueOf(value.toString());
        case "BINARY":
          return (V) (value instanceof byte[] ? value : value.toString().getBytes());
        case "STRING":
        case "VARCHAR":
        case "CHAR":
        default:
          return (V) value.toString();
      }
    } catch (Exception e) {
      LOGGER.error("Error converting simple value of type {}: {}", valueType, e.getMessage(), e);
      throw new IllegalArgumentException(
          "Failed to convert value " + value + " to type " + valueType, e);
    }
  }

  @Override
  public int size() {
    LOGGER.trace("Getting map size");
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    LOGGER.trace("Checking if map is empty");
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    LOGGER.trace("Checking if map contains key: {}", key);
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    LOGGER.trace("Checking if map contains value: {}", value);
    return map.containsValue(value);
  }

  @Override
  public V get(Object key) {
    LOGGER.trace("Getting value for key: {}", key);
    return map.get(key);
  }

  @Override
  public V put(K key, V value) {
    LOGGER.debug("Putting key: {}, value: {} in map", key, value);
    return map.put(key, value);
  }

  @Override
  public V remove(Object key) {
    LOGGER.debug("Removing key: {} from map", key);
    return map.remove(key);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    LOGGER.debug("Putting all entries from given map into current map");
    map.putAll(m);
  }

  @Override
  public void clear() {
    LOGGER.debug("Clearing map");
    map.clear();
  }

  @Override
  public java.util.Set<K> keySet() {
    LOGGER.trace("Getting key set from map");
    return map.keySet();
  }

  @Override
  public java.util.Collection<V> values() {
    LOGGER.trace("Getting values collection from map");
    return map.values();
  }

  @Override
  public java.util.Set<Entry<K, V>> entrySet() {
    LOGGER.trace("Getting entry set from map");
    return map.entrySet();
  }
}
