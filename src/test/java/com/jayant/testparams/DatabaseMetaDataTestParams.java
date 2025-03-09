package com.jayant.testparams;

import static com.jayant.testparams.ParamUtils.putInMapForKey;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;

public class DatabaseMetaDataTestParams implements TestParams {

  @Override
  public Map<Map.Entry<String, Integer>, Set<Object[]>> getFunctionToArgsMap() {
    Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap = new HashMap<>();

    for (Integer fromType : getAllSqlTypes()) {
      for (Integer toType : getAllSqlTypes()) {
        putInMapForKey(
            functionToArgsMap, Map.entry("supportsConvert", 2), new Integer[] {fromType, toType});
      }
    }

    return functionToArgsMap;
  }

  @Override
  public Set<Map.Entry<String, Integer>> getAcceptedKnownDiffs() {
    Set<Map.Entry<String, Integer>> acceptedKnownDiffs = new HashSet<>();
    // getSchemas with no args returns empty result set for SEA
    acceptedKnownDiffs.add(Map.entry("getSchemas", 0));

    // don't compare classes
    acceptedKnownDiffs.add(Map.entry("getConnection", 0));

    // don't compare driver version
    acceptedKnownDiffs.add(Map.entry("getDriverVersion", 0));

    // URL passes is different
    acceptedKnownDiffs.add(Map.entry("getURL", 0));

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
