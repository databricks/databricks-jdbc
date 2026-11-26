package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class ComplexTypePlan {
  private static final Pattern DECIMAL_TYPE =
      Pattern.compile(
          "(?:DECIMAL|DEC|NUMERIC)(?:\\s*\\(\\s*\\d+\\s*(?:,\\s*(\\d+)\\s*)?\\))?",
          Pattern.CASE_INSENSITIVE);
  static final Cache<String, ComplexTypePlan> CACHE =
      CacheBuilder.newBuilder().maximumSize(1024).build();
  static final ComplexTypePlan STRING = new ComplexTypePlan("STRING");

  final String type;
  final boolean decimal;
  final boolean containsDecimal;
  final boolean containsStruct;
  final boolean string;
  final Integer scale;
  final ComplexTypePlan element;
  final ComplexTypePlan key;
  final ComplexTypePlan value;
  final ComplexTypePlan entry;
  final Map<String, ComplexTypePlan> fields;
  final Map<String, String> fieldTypes;

  static ComplexTypePlan of(String type) {
    if (type == null || type.isEmpty()) {
      return STRING;
    }
    ComplexTypePlan plan = CACHE.getIfPresent(type);
    if (plan != null) {
      return plan;
    }
    plan = new ComplexTypePlan(type);
    ComplexTypePlan existing = CACHE.asMap().putIfAbsent(type, plan);
    return existing == null ? plan : existing;
  }

  private ComplexTypePlan(String type) {
    this.type = type;
    ComplexTypePlan element = null;
    ComplexTypePlan key = null;
    ComplexTypePlan value = null;
    Map<String, ComplexTypePlan> fields = new LinkedHashMap<>();
    Map<String, String> fieldTypes = Collections.emptyMap();
    if (type.startsWith("ARRAY<")) {
      element = of(MetadataParser.parseArrayMetadata(type));
    } else if (type.startsWith("MAP<")) {
      String[] types = MetadataParser.parseMapMetadataParts(type);
      key = of(types[0]);
      value = of(types[1]);
    } else if (type.startsWith("STRUCT<")) {
      fieldTypes = MetadataParser.parseStructMetadata(type);
      for (Map.Entry<String, String> field : fieldTypes.entrySet()) {
        fields.put(field.getKey(), of(field.getValue()));
      }
    }
    this.element = element;
    this.key = key;
    this.value = value;
    this.fields = Collections.unmodifiableMap(fields);
    this.fieldTypes = Collections.unmodifiableMap(new LinkedHashMap<>(fieldTypes));
    Matcher matcher = DECIMAL_TYPE.matcher(type);
    this.decimal = matcher.matches();
    this.scale = decimal && matcher.group(1) != null ? Integer.valueOf(matcher.group(1)) : null;
    this.string =
        DatabricksTypeUtil.recoverStringType(type) != null
            || DatabricksTypeUtil.isGeospatialType(type.toUpperCase(Locale.ROOT));
    this.containsDecimal =
        decimal
            || containsDecimal(element)
            || containsDecimal(key)
            || containsDecimal(value)
            || fields.values().stream().anyMatch(plan -> plan.containsDecimal);
    this.containsStruct =
        !fields.isEmpty()
            || type.startsWith("STRUCT<")
            || containsStruct(element)
            || containsStruct(key)
            || containsStruct(value);
    if (key != null) {
      Map<String, ComplexTypePlan> entryFields = new LinkedHashMap<>();
      entryFields.put("key", key);
      entryFields.put("value", value);
      this.entry = new ComplexTypePlan(entryFields);
    } else {
      this.entry = null;
    }
  }

  private ComplexTypePlan(Map<String, ComplexTypePlan> fields) {
    this.type = "STRUCT";
    this.fields = Collections.unmodifiableMap(fields);
    this.fieldTypes = Collections.emptyMap();
    this.decimal = false;
    this.containsDecimal = fields.values().stream().anyMatch(plan -> plan.containsDecimal);
    this.containsStruct = true;
    this.string = false;
    this.scale = null;
    this.element = null;
    this.key = null;
    this.value = null;
    this.entry = null;
  }

  private static boolean containsDecimal(ComplexTypePlan plan) {
    return plan != null && plan.containsDecimal;
  }

  private static boolean containsStruct(ComplexTypePlan plan) {
    return plan != null && plan.containsStruct;
  }

  BigDecimal decimalValue(Object value) {
    BigDecimal decimal =
        value instanceof BigDecimal ? (BigDecimal) value : new BigDecimal(value.toString());
    return scale == null || decimal.scale() == scale
        ? decimal
        : decimal.setScale(scale, RoundingMode.HALF_UP);
  }
}
