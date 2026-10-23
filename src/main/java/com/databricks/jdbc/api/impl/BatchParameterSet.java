package com.databricks.jdbc.api.impl;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Immutable, position-ordered snapshot of one prepared-statement parameter set.
 *
 * <p>This model preserves JDBC's one-based parameter indexes. Transport adapters are responsible
 * for converting them to protocol-specific wire ordinals. It does not validate parameter
 * completeness, index continuity, or consistency with other parameter sets; those validations
 * remain the backend's responsibility.
 */
public final class BatchParameterSet {

  private final List<ImmutableSqlParameter> parameters;
  private final Map<Integer, ImmutableSqlParameter> parameterBindings;

  private BatchParameterSet(List<ImmutableSqlParameter> parameters) {
    this.parameters = List.copyOf(parameters);
    Map<Integer, ImmutableSqlParameter> bindings = new LinkedHashMap<>();
    this.parameters.forEach(parameter -> bindings.put(parameter.cardinal(), parameter));
    this.parameterBindings = Collections.unmodifiableMap(bindings);
  }

  public static BatchParameterSet from(Map<Integer, ImmutableSqlParameter> parameterBindings) {
    Objects.requireNonNull(parameterBindings, "parameterBindings");
    List<ImmutableSqlParameter> orderedParameters =
        parameterBindings.entrySet().stream()
            .sorted(Comparator.comparingInt(Map.Entry::getKey))
            .map(BatchParameterSet::snapshotParameter)
            .collect(Collectors.toList());
    return new BatchParameterSet(orderedParameters);
  }

  public List<ImmutableSqlParameter> getParameters() {
    return parameters;
  }

  public Map<Integer, ImmutableSqlParameter> getParameterBindings() {
    return parameterBindings;
  }

  public int size() {
    return parameters.size();
  }

  public boolean isEmpty() {
    return parameters.isEmpty();
  }

  private static ImmutableSqlParameter snapshotParameter(
      Map.Entry<Integer, ImmutableSqlParameter> entry) {
    ImmutableSqlParameter parameter = entry.getValue();
    return ImmutableSqlParameter.builder()
        .cardinal(entry.getKey())
        .type(parameter.type())
        .value(snapshotValue(parameter.value()))
        .build();
  }

  private static Object snapshotValue(Object value) {
    if (value instanceof Timestamp) {
      Timestamp timestamp = (Timestamp) value;
      Timestamp copy = new Timestamp(timestamp.getTime());
      copy.setNanos(timestamp.getNanos());
      return copy;
    }
    if (value instanceof Date) {
      return new Date(((Date) value).getTime());
    }
    if (value instanceof Time) {
      return new Time(((Time) value).getTime());
    }
    if (value instanceof byte[]) {
      return ((byte[]) value).clone();
    }
    return value;
  }
}
