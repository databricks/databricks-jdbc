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
 * Configurable filter for skipping or selecting specific DatabaseMetaData argument combinations.
 *
 * <p>Reads a JSON config file via {@code -DMETADATA_FILTER_CONFIG=path}. Supports two filter types:
 *
 * <ul>
 *   <li>{@code metadataRunOnlyFilters} — only run argument combinations matching at least one
 *       pattern (whitelist)
 *   <li>{@code metadataSkipFilters} — skip argument combinations matching any pattern (blacklist)
 * </ul>
 *
 * <p>If both are present for a method, runOnly takes precedence: first the argument combination
 * must match a runOnly pattern, then it must not match any skip pattern.
 *
 * <p>Within each pattern list: an argument combination matches if ANY pattern matches (OR). Within
 * a pattern, ALL conditions must match (AND). Prefix a value with {@code !} for negation (e.g.,
 * {@code "!"} means "not empty", {@code "!null"} means "not null").
 *
 * <p>Example config:
 *
 * <pre>{@code
 * {
 *   "metadataRunOnlyFilters": {
 *     "getTables": [
 *       {"catalog": "comparator_tests", "schemaPattern": "oss_jdbc_tests"}
 *     ]
 *   },
 *   "metadataSkipFilters": {
 *     "getTables": [
 *       {"types": "[]"}
 *     ]
 *   }
 * }
 * }</pre>
 */
public class MetadataFilterConfig {

  private static final String CONFIG_PROPERTY = "METADATA_FILTER_CONFIG";

  private final Map<String, List<Map<String, String>>> metadataRunOnlyFilters;
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

  private MetadataFilterConfig(
      Map<String, List<Map<String, String>>> metadataRunOnlyFilters,
      Map<String, List<Map<String, String>>> metadataSkipFilters) {
    this.metadataRunOnlyFilters = metadataRunOnlyFilters;
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
      Map<String, List<Map<String, String>>> runOnly =
          (Map<String, List<Map<String, String>>>) root.get("metadataRunOnlyFilters");
      @SuppressWarnings("unchecked")
      Map<String, List<Map<String, String>>> skip =
          (Map<String, List<Map<String, String>>>) root.get("metadataSkipFilters");

      if (runOnly == null) runOnly = Collections.emptyMap();
      if (skip == null) skip = Collections.emptyMap();

      System.out.println(
          "[MetadataFilter] Loaded filter config from "
              + path
              + " (runOnly: "
              + runOnly.size()
              + " methods, skip: "
              + skip.size()
              + " methods)");
      return new MetadataFilterConfig(runOnly, skip);
    } catch (IOException e) {
      System.err.println(
          "[MetadataFilter] WARNING: Failed to load config from " + path + ": " + e.getMessage());
      return empty();
    }
  }

  /** Returns an empty config that never skips anything. */
  public static MetadataFilterConfig empty() {
    return new MetadataFilterConfig(Collections.emptyMap(), Collections.emptyMap());
  }

  /**
   * Returns true if this argument combination should be skipped based on the filter config.
   *
   * <p>If runOnly patterns exist for the method, the argument combination must match at least one
   * to survive. Then, if skip patterns exist, the argument combination is skipped if it matches
   * any. RunOnly takes precedence.
   */
  public boolean shouldSkip(String methodName, Object[] args) {
    List<String> argNames = ARG_NAMES.get(methodName);
    if (argNames == null || argNames.isEmpty()) {
      return false;
    }

    Map<String, String> namedArgs = toNamedArgs(args, argNames);

    // RunOnly takes precedence: if defined for this method, argument combination must match at
    // least one pattern
    List<Map<String, String>> runOnlyPatterns = metadataRunOnlyFilters.get(methodName);
    if (runOnlyPatterns != null && !runOnlyPatterns.isEmpty()) {
      if (!matchesAnyPattern(namedArgs, runOnlyPatterns)) {
        return true; // not in the whitelist → skip
      }
    }

    // Then apply skip filters
    List<Map<String, String>> skipPatterns = metadataSkipFilters.get(methodName);
    if (skipPatterns != null && !skipPatterns.isEmpty()) {
      if (matchesAnyPattern(namedArgs, skipPatterns)) {
        return true; // in the blacklist → skip
      }
    }

    return false;
  }

  /** Whether this config has any filters defined. */
  public boolean isEmpty() {
    return metadataRunOnlyFilters.isEmpty() && metadataSkipFilters.isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  private static boolean matchesAnyPattern(
      Map<String, String> namedArgs, List<Map<String, String>> patterns) {
    for (Map<String, String> pattern : patterns) {
      if (matchesPattern(namedArgs, pattern)) {
        return true; // OR across patterns
      }
    }
    return false;
  }

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
      if (skipValue.startsWith("!")) {
        // Negation: "!value" means arg must NOT equal value
        if (skipValue.substring(1).equals(actual)) {
          return false;
        }
      } else {
        if (!skipValue.equals(actual)) {
          return false; // AND within pattern
        }
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
