package com.databricks.jdbc.api.impl;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Locale;
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
    return delegate.get(normalize(key));
  }

  public V getOrDefault(String key, V defaultValue) {
    return delegate.getOrDefault(normalize(key), defaultValue);
  }

  public boolean containsKey(String key) {
    return delegate.containsKey(normalize(key));
  }

  public int size() {
    return delegate.size();
  }

  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  // Locale.ROOT so lookups don't depend on the default locale (e.g. Turkish "ID" -> "ıd").
  private static String normalize(String key) {
    return key.toLowerCase(Locale.ROOT);
  }

  static final class Builder<V> {
    private final Map<String, V> entries = new HashMap<>();

    private Builder() {}

    /**
     * Adds the entry unless a key differing only in case was already added, so the first one wins.
     */
    public Builder<V> putIfAbsent(String key, V value) {
      entries.putIfAbsent(normalize(key), value);
      return this;
    }

    public CaseInsensitiveImmutableMap<V> build() {
      return new CaseInsensitiveImmutableMap<>(ImmutableMap.copyOf(entries));
    }
  }
}
