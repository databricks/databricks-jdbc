package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import java.util.LinkedHashMap;

public class ConverterHelperProvider {
  static final LinkedHashMap<String, ConverterHelper> converterHelpers = new LinkedHashMap<>();

  public static ConverterHelper getConverterHelper(IDatabricksConnectionContext connectionContext) {
    return converterHelpers.computeIfAbsent(
        connectionContext.getConnectionUuid(), k -> new ConverterHelper(connectionContext));
  }
}
