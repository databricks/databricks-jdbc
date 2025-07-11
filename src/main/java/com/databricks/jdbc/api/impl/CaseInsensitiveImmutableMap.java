package com.databricks.jdbc.api.impl;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * Internal utility class for case-insensitive immutable maps. Keys are normalized to lowercase
 * during construction for efficient case-insensitive lookups.
 */
class CaseInsensitiveImmutableMap<V> {
  private final ImmutableMap<String, V> delegate;

  private CaseInsensitiveImmutableMap(ImmutableMap<String, V> delegate) {
    this.delegate = delegate;
  }

  public static <V> CaseInsensitiveImmutableMap<V> copyOf(Map<String, V> map) {
    ImmutableMap<String, V> normalizedMap =
        map.entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    entry -> entry.getKey().toLowerCase(), Map.Entry::getValue));

    return new CaseInsensitiveImmutableMap<>(normalizedMap);
  }

  public V get(String key) {
    return delegate.get(key.toLowerCase());
  }

  public V getOrDefault(String key, V defaultValue) {
    return delegate.getOrDefault(key.toLowerCase(), defaultValue);
  }

  public boolean containsKey(String key) {
    return delegate.containsKey(key.toLowerCase());
  }

  public int size() {
    return delegate.size();
  }

  public boolean isEmpty() {
    return delegate.isEmpty();
  }
}
