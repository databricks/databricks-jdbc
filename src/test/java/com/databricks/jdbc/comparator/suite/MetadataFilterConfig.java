package com.databricks.jdbc.comparator.suite;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configurable filter for skipping specific DatabaseMetaData argument combinations.
 *
 * <p>Reads a JSON config file via {@code -DMETADATA_FILTER_CONFIG=path}. Each method can have
 * multiple filter patterns. A combo is skipped if ANY pattern matches (OR). Within a pattern, ALL
 * conditions must match (AND).
 *
 * <p>Example config:
 *
 * <pre>{@code
 * {
 *   "metadataSkipFilters": {
 *     "getTables": [
 *       {"schemaPattern": ""},
 *       {"types": "[]"}
 *     ],
 *     "getSchemas": [
 *       {"schemaPattern": ""}
 *     ]
 *   }
 * }
 * }</pre>
 */
public class MetadataFilterConfig {

  private static final String CONFIG_PROPERTY = "METADATA_FILTER_CONFIG";

  private final Map<String, List<Map<String, String>>> metadataSkipFilters;

  // Arg name → position mapping per method (only methods with filterable args)
  private static final Map<String, List<String>> ARG_NAMES =
      Map.ofEntries(
          Map.entry("getCatalogs", List.of()),
          Map.entry("getSchemas", List.of("catalog", "schemaPattern")),
          Map.entry("getTables", List.of("catalog", "schemaPattern", "tableNamePattern", "types")),
          Map.entry(
              "getColumns",
              List.of("catalog", "schemaPattern", "tableNamePattern", "columnNamePattern")),
          Map.entry("getPrimaryKeys", List.of("catalog", "schema", "table")),
          Map.entry("getImportedKeys", List.of("catalog", "schema", "table")),
          Map.entry("getExportedKeys", List.of("catalog", "schema", "table")),
          Map.entry(
              "getCrossReference",
              List.of(
                  "parentCatalog",
                  "parentSchema",
                  "parentTable",
                  "foreignCatalog",
                  "foreignSchema",
                  "foreignTable")),
          Map.entry("getFunctions", List.of("catalog", "schemaPattern", "functionNamePattern")));

  private MetadataFilterConfig(Map<String, List<Map<String, String>>> metadataSkipFilters) {
    this.metadataSkipFilters = metadataSkipFilters;
  }

  /** Loads filter config from the system property path, or returns an empty (no-op) config. */
  public static MetadataFilterConfig load() {
    String path = System.getProperty(CONFIG_PROPERTY);
    System.out.println("[MetadataFilter] Config property: " + path);
    if (path == null || path.isEmpty()) {
      return empty();
    }
    try {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> root =
          mapper.readValue(new File(path), new TypeReference<Map<String, Object>>() {});

      @SuppressWarnings("unchecked")
      Map<String, List<Map<String, String>>> filters =
          (Map<String, List<Map<String, String>>>) root.get("metadataSkipFilters");

      if (filters == null) {
        filters = Collections.emptyMap();
      }
      System.out.println(
          "[MetadataFilter] Loaded filter config from "
              + path
              + " ("
              + filters.size()
              + " methods)");
      return new MetadataFilterConfig(filters);
    } catch (IOException e) {
      System.err.println(
          "[MetadataFilter] WARNING: Failed to load config from " + path + ": " + e.getMessage());
      return empty();
    }
  }

  /** Returns an empty config that never skips anything. */
  public static MetadataFilterConfig empty() {
    return new MetadataFilterConfig(Collections.emptyMap());
  }

  /** Returns true if this combo should be skipped based on the filter config. */
  public boolean shouldSkip(String methodName, Object[] args) {
    List<Map<String, String>> patterns = metadataSkipFilters.get(methodName);
    if (patterns == null || patterns.isEmpty()) {
      return false;
    }

    List<String> argNames = ARG_NAMES.get(methodName);
    if (argNames == null || argNames.isEmpty()) {
      return false;
    }

    Map<String, String> namedArgs = toNamedArgs(args, argNames);

    for (Map<String, String> pattern : patterns) {
      if (matchesPattern(namedArgs, pattern)) {
        return true; // OR across patterns
      }
    }
    return false;
  }

  /** Whether this config has any filters defined. */
  public boolean isEmpty() {
    return metadataSkipFilters.isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  private static Map<String, String> toNamedArgs(Object[] args, List<String> argNames) {
    Map<String, String> named = new HashMap<>();
    for (int i = 0; i < Math.min(args.length, argNames.size()); i++) {
      named.put(argNames.get(i), argToString(args[i]));
    }
    return named;
  }

  private static boolean matchesPattern(
      Map<String, String> namedArgs, Map<String, String> pattern) {
    for (Map.Entry<String, String> condition : pattern.entrySet()) {
      String argName = condition.getKey();
      String skipValue = condition.getValue();
      String actual = namedArgs.get(argName);
      if (!skipValue.equals(actual)) {
        return false; // AND within pattern
      }
    }
    return true;
  }

  private static String argToString(Object arg) {
    if (arg == null) return "null";
    if (arg instanceof String[]) return Arrays.toString((String[]) arg);
    if (arg instanceof Object[]) return Arrays.toString((Object[]) arg);
    return arg.toString();
  }
}
