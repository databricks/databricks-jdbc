package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class CaseInsensitiveImmutableMapTest {

  @Test
  public void testKeysDifferingOnlyInCaseKeepFirstEntry() {
    CaseInsensitiveImmutableMap<Integer> map =
        CaseInsensitiveImmutableMap.<Integer>builder()
            .putIfAbsent("a", 1)
            .putIfAbsent("A", 2)
            .putIfAbsent("b", 3)
            .build();

    assertEquals(2, map.size());
    assertEquals(1, map.get("a"));
    assertEquals(1, map.get("A"));
    assertEquals(3, map.get("B"));
    assertEquals(-1, map.getOrDefault("c", -1));
  }
}
