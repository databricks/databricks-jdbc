package com.databricks.jdbc.api.impl;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DatabricksMap<K, V> implements Map<K, V> {
  private final Map<K, V> map;

  public DatabricksMap(Map<K, V> map, String metadata) {
    this.map = convertMap(map, metadata);
  }

  private Map<K, V> convertMap(Map<K, V> originalMap, String metadata) {
    Map<K, V> convertedMap = new LinkedHashMap<>();
    String[] mapMetadata = MetadataParser.parseMapMetadata(metadata).split(",", 2);
    String keyType = mapMetadata[0].trim();
    String valueType = mapMetadata[1].trim();

    for (Map.Entry<K, V> entry : originalMap.entrySet()) {
      K key = convertKey(entry.getKey(), keyType);
      V value = convertValue(entry.getValue(), valueType);
      convertedMap.put(key, value);
    }

    return convertedMap;
  }

  private K convertKey(K key, String keyType) {
    return key;
  }

  private V convertValue(V value, String valueType) {
    if (valueType.startsWith("STRUCT")) {
      if (value instanceof Map) {
        return (V) new DatabricksStruct((Map<String, Object>) value, valueType);
      } else {
        throw new IllegalArgumentException(
                "Expected a Map for STRUCT but found: " + value.getClass().getSimpleName());
      }
    } else if (valueType.startsWith("ARRAY")) {
      if (value instanceof List) {
        return (V) new DatabricksArray((List<Object>) value, valueType);
      } else {
        throw new IllegalArgumentException(
                "Expected a List for ARRAY but found: " + value.getClass().getSimpleName());
      }
    } else if (valueType.startsWith("MAP")) {
      if (value instanceof Map) {
        return (V) new DatabricksMap<>((Map<String, Object>) value, valueType);
      } else {
        throw new IllegalArgumentException(
                "Expected a Map for MAP but found: " + value.getClass().getSimpleName());
      }
    } else {
      return convertSimpleValue(value, valueType);
    }
  }

  private V convertSimpleValue(V value, String valueType) {
    if (value == null) {
      return null;
    }

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
}
