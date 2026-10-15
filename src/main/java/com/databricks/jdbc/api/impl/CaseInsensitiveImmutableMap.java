package com.databricks.jdbc.api.impl;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
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

  public static <V> Builder<V> builder() {
    return new Builder<>();
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

  static final class Builder<V> {
    private final Map<String, V> entries = new HashMap<>();

    private Builder() {}

    /**
     * Adds the entry unless a key differing only in case was already added, so the first one wins.
     */
    public Builder<V> putIfAbsent(String key, V value) {
      entries.putIfAbsent(key.toLowerCase(), value);
      return this;
    }

    public CaseInsensitiveImmutableMap<V> build() {
      return new CaseInsensitiveImmutableMap<>(ImmutableMap.copyOf(entries));
    }
  }
}
