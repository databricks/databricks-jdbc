package com.jayant;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;

public class DatabaseMetaDataTestParams {
  private static List<Integer> allSqlTypes = getAllSqlTypes();
  private static List<Integer> allTransactionIsolationLevels = getAllTransactionIsolationLevels();

  public static Map<Map.Entry<String, Integer>, Object[]> getFunctionToArgsMap() {
    Map<Map.Entry<String, Integer>, Object[]> functionToArgsMap = new HashMap<>();
    functionToArgsMap.put(
        Map.entry("getTables", 4), new String[] {"main", "tpcds_sf100_delta", "%", null});
    functionToArgsMap.put(
        Map.entry("getTablePrivileges", 3), new String[] {"main", "tpcds_sf100_delta", "%"});
    functionToArgsMap.put(Map.entry("getSchemas", 2), new String[] {"main", "tpcds_%"});
    functionToArgsMap.put(
        Map.entry("getColumns", 4),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales", "%"});
    functionToArgsMap.put(
        Map.entry("getColumnPrivileges", 4),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales", "%"});
    functionToArgsMap.put(
        Map.entry("getVersionColumns", 3),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales"});
    functionToArgsMap.put(
        Map.entry("getFunctions", 3), new String[] {"main", "tpcds_sf100_delta", "aggregate"});
    functionToArgsMap.put(
        Map.entry("getProcedures", 3), new String[] {"main", "tpcds_sf100_delta", "%"});
    functionToArgsMap.put(
        Map.entry("getProcedureColumns", 4), new String[] {"main", "tpcds_sf100_delta", "%", "%"});
    functionToArgsMap.put(
        Map.entry("getPrimaryKeys", 3),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales"});
    functionToArgsMap.put(
        Map.entry("getImportedKeys", 3),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales"});
    functionToArgsMap.put(
        Map.entry("getExportedKeys", 3),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales"});
    // TODO: Add a proper cross reference test
    functionToArgsMap.put(
        Map.entry("getCrossReference", 3),
        new String[] {
          "main", "tpcds_sf100_delta", "catalog_sales", "main", "tpcds_sf100_delta", "catalog_sales"
        });
    functionToArgsMap.put(
        Map.entry("getIndexInfo", 5),
        new Object[] {"main", "tpcds_sf100_delta", "catalog_sales", true, false});
    functionToArgsMap.put(
        Map.entry("supportsResultSetType", 1),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales", "true"});
    functionToArgsMap.put(
        Map.entry("getIndexInfo", 4),
        new String[] {"main", "tpcds_sf100_delta", "catalog_sales", "true"});

    for (Integer type : allSqlTypes) {
      for (Integer type2 : allSqlTypes) {
        functionToArgsMap.put(Map.entry("supportsConvert", 2), new Integer[] {type, type2});
        functionToArgsMap.put(Map.entry("supportsConvert", 2), new Integer[] {type2, type});
      }
    }
    for (Integer i : allTransactionIsolationLevels) {
      functionToArgsMap.put(Map.entry("supportsTransactionIsolationLevel", 1), new Integer[] {i});
    }
    for (Integer i : getAllBestRowIdentifierScopes()) {
      functionToArgsMap.put(Map.entry("getBestRowIdentifier", 1), new Integer[] {i});
    }
    for (Integer i : getResultSetTypes()) {
      functionToArgsMap.put(Map.entry("supportsResultSetType", 1), new Integer[] {i});
    }

    return functionToArgsMap;
  }

  public static Set<Map.Entry<String, Integer>> getAcceptedKnownDiffs() {
    Set<Map.Entry<String, Integer>> acceptedKnownDiffs = new HashSet<>();
    // getSchemas with no args returns empty result set for SEA
    acceptedKnownDiffs.add(Map.entry("getSchemas", 0));
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
}
