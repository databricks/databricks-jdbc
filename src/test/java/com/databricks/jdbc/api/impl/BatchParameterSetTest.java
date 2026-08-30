package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BatchParameterSetTest {

  @Test
  void ordersParametersAndPreservesJdbcIndexes() {
    Map<Integer, ImmutableSqlParameter> bindings = new HashMap<>();
    bindings.put(3, parameter(99, "third", ColumnInfoTypeName.STRING));
    bindings.put(1, parameter(99, "first", ColumnInfoTypeName.STRING));
    bindings.put(2, parameter(99, "second", ColumnInfoTypeName.STRING));

    BatchParameterSet parameterSet = BatchParameterSet.from(bindings);

    assertEquals(List.of("first", "second", "third"), values(parameterSet));
    assertEquals(List.of(1, 2, 3), indexes(parameterSet));
    assertEquals(List.of(1, 2, 3), List.copyOf(parameterSet.getParameterBindings().keySet()));
  }

  @Test
  void preservesSparseIndexesWithoutValidation() {
    Map<Integer, ImmutableSqlParameter> bindings = new HashMap<>();
    bindings.put(3, parameter(3, "third", ColumnInfoTypeName.STRING));
    bindings.put(1, parameter(1, "first", ColumnInfoTypeName.STRING));

    BatchParameterSet parameterSet = BatchParameterSet.from(bindings);

    assertEquals(List.of("first", "third"), values(parameterSet));
    assertEquals(List.of(1, 3), indexes(parameterSet));
  }

  @Test
  void allowsEmptyParameterSet() {
    BatchParameterSet parameterSet = BatchParameterSet.from(Map.of());

    assertTrue(parameterSet.isEmpty());
    assertEquals(0, parameterSet.size());
  }

  @Test
  void snapshotsBindingsAndMutableValues() {
    Timestamp timestamp = Timestamp.valueOf("2026-08-10 12:34:56.123456789");
    byte[] bytes = new byte[] {1, 2, 3};
    Map<Integer, ImmutableSqlParameter> bindings = new HashMap<>();
    bindings.put(1, parameter(1, timestamp, ColumnInfoTypeName.TIMESTAMP));
    bindings.put(2, parameter(2, bytes, ColumnInfoTypeName.BINARY));

    BatchParameterSet parameterSet = BatchParameterSet.from(bindings);
    bindings.clear();
    timestamp.setTime(0);
    bytes[0] = 9;

    assertFalse(parameterSet.isEmpty());
    assertEquals(
        Timestamp.valueOf("2026-08-10 12:34:56.123456789"),
        parameterSet.getParameters().get(0).value());
    assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) parameterSet.getParameters().get(1).value());
    assertThrows(
        UnsupportedOperationException.class,
        () -> parameterSet.getParameters().add(parameter(3, "extra", ColumnInfoTypeName.STRING)));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            parameterSet
                .getParameterBindings()
                .put(3, parameter(3, "extra", ColumnInfoTypeName.STRING)));
  }

  @Test
  void preservesNullValueAndType() {
    BatchParameterSet parameterSet =
        BatchParameterSet.from(Map.of(1, parameter(1, null, ColumnInfoTypeName.DECIMAL)));

    ImmutableSqlParameter parameter = parameterSet.getParameters().get(0);
    assertNull(parameter.value());
    assertEquals(ColumnInfoTypeName.DECIMAL, parameter.type());
    assertEquals(1, parameter.cardinal());
  }

  private ImmutableSqlParameter parameter(
      int cardinal, Object value, ColumnInfoTypeName columnInfoTypeName) {
    return ImmutableSqlParameter.builder()
        .cardinal(cardinal)
        .value(value)
        .type(columnInfoTypeName)
        .build();
  }

  private List<Object> values(BatchParameterSet parameterSet) {
    return parameterSet.getParameters().stream()
        .map(ImmutableSqlParameter::value)
        .collect(java.util.stream.Collectors.toList());
  }

  private List<Integer> indexes(BatchParameterSet parameterSet) {
    return parameterSet.getParameters().stream()
        .map(ImmutableSqlParameter::cardinal)
        .collect(java.util.stream.Collectors.toList());
  }
}
