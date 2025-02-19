package com.jayant.testparams;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;

public class DatabaseMetaDataTestParams implements TestParams {

  private void putInMapForKey(
      Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap,
      Map.Entry<String, Integer> key,
      Object[] value) {
    functionToArgsMap.putIfAbsent(key, new HashSet<>());
    functionToArgsMap.get(key).add(value);
  }

  @Override
  public Map<Map.Entry<String, Integer>, Set<Object[]>> getFunctionToArgsMap() {
    Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap = new HashMap<>();

    putInMapForKey(
        functionToArgsMap,
        Map.entry("getTables", 4),
        new String[] {"main", "tpcds_sf100_delta", "%", null});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getTablePrivileges", 3),
        new String[] {"main", "tpcds_sf100_delta", "%"});
    putInMapForKey(functionToArgsMap, Map.entry("getSchemas", 2), new String[] {"main", "tpcds_%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getColumns", 4),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales", "%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getPseudoColumns", 4),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales", "%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getColumnPrivileges", 4),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales", "%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getVersionColumns", 3),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getFunctions", 3),
        new String[] {"main", "tpcds_sf100_delta", "aggregate"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getFunctionColumns", 4),
        new String[] {"main", "tpcds_sf100_delta", "aggregate", "%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getProcedures", 3),
        new String[] {"main", "tpcds_sf100_delta", "%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getProcedureColumns", 4),
        new String[] {"main", "tpcds_sf100_delta", "%", "%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getPrimaryKeys", 3),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getImportedKeys", 3),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getExportedKeys", 3),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales"});
    // TODO: Add a proper cross reference test
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getCrossReference", 6),
        new String[] {
          "main", "tpcds_sf100_delta", "catalog_sales", "main", "tpcds_sf100_delta", "catalog_sales"
        });
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getIndexInfo", 5),
        new Object[] {"main", "tpcds_sf100_delta", "catalog_sales", true, false});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getUDTs", 4),
        new String[] {"main", "tpcds_sf100_delta", "%", null});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getSuperTypes", 3),
        new String[] {"main", "tpcds_sf100_delta", "%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getSuperTables", 3),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getAttributes", 4),
        new String[] {"main", "tpcds_sf100_delta", "%", "%"});

    // Methods for ResultSet concurrency and visibility
    for (Integer type : getResultSetTypes()) {
      putInMapForKey(
          functionToArgsMap, Map.entry("supportsResultSetType", 1), new Integer[] {type});
      putInMapForKey(
          functionToArgsMap,
          Map.entry("supportsResultSetConcurrency", 2),
          new Integer[] {type, ResultSet.CONCUR_READ_ONLY});
      putInMapForKey(
          functionToArgsMap,
          Map.entry("supportsResultSetConcurrency", 2),
          new Integer[] {type, ResultSet.CONCUR_UPDATABLE});
      putInMapForKey(functionToArgsMap, Map.entry("ownUpdatesAreVisible", 1), new Integer[] {type});
      putInMapForKey(functionToArgsMap, Map.entry("ownDeletesAreVisible", 1), new Integer[] {type});
      putInMapForKey(functionToArgsMap, Map.entry("ownInsertsAreVisible", 1), new Integer[] {type});
      putInMapForKey(
          functionToArgsMap, Map.entry("othersUpdatesAreVisible", 1), new Integer[] {type});
      putInMapForKey(
          functionToArgsMap, Map.entry("othersDeletesAreVisible", 1), new Integer[] {type});
      putInMapForKey(
          functionToArgsMap, Map.entry("othersInsertsAreVisible", 1), new Integer[] {type});
      putInMapForKey(functionToArgsMap, Map.entry("updatesAreDetected", 1), new Integer[] {type});
      putInMapForKey(functionToArgsMap, Map.entry("deletesAreDetected", 1), new Integer[] {type});
      putInMapForKey(functionToArgsMap, Map.entry("insertsAreDetected", 1), new Integer[] {type});
    }

    for (Integer type : getAllSqlTypes()) {
      for (Integer type2 : getAllSqlTypes()) {
        putInMapForKey(
            functionToArgsMap, Map.entry("supportsConvert", 2), new Integer[] {type, type2});
        putInMapForKey(
            functionToArgsMap, Map.entry("supportsConvert", 2), new Integer[] {type2, type});
      }
    }
    for (Integer i : getAllTransactionIsolationLevels()) {
      putInMapForKey(
          functionToArgsMap, Map.entry("supportsTransactionIsolationLevel", 1), new Integer[] {i});
    }
    for (Integer i : getAllBestRowIdentifierScopes()) {
      putInMapForKey(
          functionToArgsMap,
          Map.entry("getBestRowIdentifier", 5),
          new Object[] {"main", "tpcds_sf100_delta", "catalog_sales", i, true});
    }
    for (Integer i : getResultSetHoldability()) {
      putInMapForKey(
          functionToArgsMap, Map.entry("supportsResultSetHoldability", 1), new Integer[] {i});
    }

    return functionToArgsMap;
  }

  @Override
  public Set<Map.Entry<String, Integer>> getAcceptedKnownDiffs() {
    Set<Map.Entry<String, Integer>> acceptedKnownDiffs = new HashSet<>();
    // getSchemas with no args returns empty result set for SEA
    acceptedKnownDiffs.add(Map.entry("getSchemas", 0));

    // Methods that we do not need to test from the Super class
    acceptedKnownDiffs.add(Map.entry("unwrap", 1));
    acceptedKnownDiffs.add(Map.entry("isWrapperFor", 1));
    return acceptedKnownDiffs;
  }

  private static List<Integer> getAllSqlTypes() {
    List<Integer> sqlTypes = new ArrayList<>();

    // Get all fields from the Types class
    Field[] fields = Types.class.getFields();

    for (Field field : fields) {
      if (field.getType().equals(int.class)) { // Only consider fields of type int (SQL types)
        try {
          // Add each constant value to the list
          sqlTypes.add((Integer) field.get(null));
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
      }
    }

    return sqlTypes;
  }

  private static List<Integer> getAllTransactionIsolationLevels() {
    return new ArrayList<>(
        Arrays.asList(
            Connection.TRANSACTION_NONE,
            Connection.TRANSACTION_READ_UNCOMMITTED,
            Connection.TRANSACTION_READ_COMMITTED,
            Connection.TRANSACTION_REPEATABLE_READ,
            Connection.TRANSACTION_SERIALIZABLE));
  }

  private static List<Integer> getAllBestRowIdentifierScopes() {
    return new ArrayList<>(
        Arrays.asList(
            DatabaseMetaData.bestRowTemporary,
            DatabaseMetaData.bestRowTransaction,
            DatabaseMetaData.bestRowSession));
  }

  private static List<Integer> getResultSetTypes() {
    return new ArrayList<>(
        Arrays.asList(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.TYPE_SCROLL_SENSITIVE));
  }

  private static List<Integer> getResultSetHoldability() {
    return new ArrayList<>(
        Arrays.asList(ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT));
  }
}
