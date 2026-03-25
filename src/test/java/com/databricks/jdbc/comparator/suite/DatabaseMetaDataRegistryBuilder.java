package com.databricks.jdbc.comparator.suite;

import java.lang.reflect.Method;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builder for constructing a DatabaseMetaData method test registry.
 *
 * <p>Provides a fluent API for registering methods with different argument patterns:
 *
 * <pre>
 *   r.method("getSchemas").allCombinationsOf(CATALOG, SCHEMA);
 *   r.method("getFunctionColumns").stub("cat", "sch", "func", "%");
 *   r.method("getCrossReference").explicit(crossRefCases);
 *   r.method("getCatalogs");  // 0-arg
 *   r.discoverRemainingZeroArgMethods();
 * </pre>
 *
 * <p>All registration methods automatically detect and include 0-arg overloads on {@link
 * DatabaseMetaData}, so overloads like {@code getSchemas()} and {@code getSchemas(String, String)}
 * are never silently dropped.
 */
public class DatabaseMetaDataRegistryBuilder {

  private final Map<String, List<Object[]>> registry = new LinkedHashMap<>();
  private final Set<String> skippedMethods = new HashSet<>();
  private Set<String> runOnlyMethodsFilter = null;

  // Pending method from the last method() call, flushed on next method() or build()
  private String pendingMethod;

  private static final Set<String> OBJECT_METHODS =
      Set.of(
          "toString",
          "hashCode",
          "equals",
          "getClass",
          "notify",
          "notifyAll",
          "wait",
          "clone",
          "finalize");

  /** Methods to skip during {@link #discoverRemainingZeroArgMethods()}. */
  public DatabaseMetaDataRegistryBuilder skipMethods(String... methods) {
    skippedMethods.addAll(Arrays.asList(methods));
    return this;
  }

  /** If set, {@link #build()} filters the registry to only these methods. */
  public DatabaseMetaDataRegistryBuilder runOnly(String... methods) {
    runOnlyMethodsFilter = new HashSet<>(Arrays.asList(methods));
    return this;
  }

  /**
   * Starts registration for a method. If no chain follows (no {@link #allCombinationsOf}, {@link
   * #stub}, or {@link #explicit}), the method is registered as 0-arg when the next {@link #method}
   * or {@link #build} is called.
   */
  public DatabaseMetaDataRegistryBuilder method(String name) {
    flushPending();
    pendingMethod = name;
    return this;
  }

  /** Registers all cartesian product combinations of the given variant lists. */
  @SafeVarargs
  public final DatabaseMetaDataRegistryBuilder allCombinationsOf(List<Object>... variantLists) {
    String name = consumePending();
    List<Object[]> combos = new ArrayList<>();
    if (hasZeroArgOverload(name)) {
      combos.add(new Object[0]);
    }
    combos.addAll(cartesianProduct(variantLists));
    registry.put(name, combos);
    return this;
  }

  /**
   * Registers a single explicit arg set. Convenient for stub methods that return empty ResultSet.
   */
  public DatabaseMetaDataRegistryBuilder stub(Object... args) {
    String name = consumePending();
    List<Object[]> combos = new ArrayList<>();
    if (hasZeroArgOverload(name)) {
      combos.add(new Object[0]);
    }
    combos.add(args);
    registry.put(name, combos);
    return this;
  }

  /** Registers a curated list of arg combos (for non-cartesian cases). */
  public DatabaseMetaDataRegistryBuilder explicit(List<Object[]> combos) {
    String name = consumePending();
    List<Object[]> allCombos = new ArrayList<>();
    if (hasZeroArgOverload(name)) {
      boolean hasZeroArg = combos.stream().anyMatch(a -> a.length == 0);
      if (!hasZeroArg) {
        allCombos.add(new Object[0]);
      }
    }
    allCombos.addAll(combos);
    registry.put(name, allCombos);
    return this;
  }

  /**
   * Auto-discovers all 0-arg public methods on DatabaseMetaData not yet registered. Call this last
   * — it catches remaining zero-arg methods (getTableTypes, getTypeInfo, ~100 scalar methods).
   */
  public DatabaseMetaDataRegistryBuilder discoverRemainingZeroArgMethods() {
    flushPending();
    for (Method m : DatabaseMetaData.class.getMethods()) {
      String name = m.getName();
      if (m.getParameterCount() != 0) continue;
      if (registry.containsKey(name)) continue;
      if (skippedMethods.contains(name)) continue;
      if (OBJECT_METHODS.contains(name)) continue;
      registry.put(name, Collections.singletonList(new Object[0]));
    }
    return this;
  }

  /** Returns the completed registry, filtered by {@link #runOnly} if set. */
  public Map<String, List<Object[]>> build() {
    flushPending();
    if (runOnlyMethodsFilter != null) {
      registry.keySet().retainAll(runOnlyMethodsFilter);
    }
    return registry;
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  /** Flushes a pending method with no chain as a 0-arg registration. */
  private void flushPending() {
    if (pendingMethod != null) {
      registry.put(pendingMethod, Collections.singletonList(new Object[0]));
      pendingMethod = null;
    }
  }

  /** Consumes the pending method name for use by a chained call. */
  private String consumePending() {
    if (pendingMethod == null) {
      throw new IllegalStateException("No method() call before allCombinationsOf/stub/explicit");
    }
    String name = pendingMethod;
    pendingMethod = null;
    return name;
  }

  /** Checks if DatabaseMetaData has a 0-arg overload for the given method name. */
  private static boolean hasZeroArgOverload(String methodName) {
    for (Method m : DatabaseMetaData.class.getMethods()) {
      if (m.getName().equals(methodName) && m.getParameterCount() == 0) {
        return true;
      }
    }
    return false;
  }

  /** Returns the cartesian product of the given variant lists. */
  @SafeVarargs
  private static List<Object[]> cartesianProduct(List<Object>... variantLists) {
    List<Object[]> result = new ArrayList<>();
    result.add(new Object[0]);
    for (List<Object> variants : variantLists) {
      List<Object[]> next = new ArrayList<>();
      for (Object[] prefix : result) {
        for (Object val : variants) {
          Object[] combo = Arrays.copyOf(prefix, prefix.length + 1);
          combo[prefix.length] = val;
          next.add(combo);
        }
      }
      result = next;
    }
    return result;
  }
}
