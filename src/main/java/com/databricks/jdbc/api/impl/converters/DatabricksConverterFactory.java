package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import java.util.LinkedHashMap;

public class DatabricksConverterFactory {
  final LinkedHashMap<String, ConverterHelper> instances = new LinkedHashMap<>();
  private static final DatabricksConverterFactory INSTANCE = new DatabricksConverterFactory();

  public static DatabricksConverterFactory getInstance() {
    return INSTANCE;
  }

  public ConverterHelper getConverterHelper(IDatabricksConnectionContext context) {
    return instances.computeIfAbsent(
        context.getConnectionUuid(), k -> new ConverterHelper(context));
  }
}
