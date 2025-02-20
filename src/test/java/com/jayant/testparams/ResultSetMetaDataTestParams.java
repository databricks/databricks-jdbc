package com.jayant.testparams;

import static com.jayant.testparams.ParamUtils.putInMapForKey;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ResultSetMetaDataTestParams implements TestParams {

  @Override
  public Set<Map.Entry<String, Integer>> getAcceptedKnownDiffs() {
    Set<Map.Entry<String, Integer>> acceptedKnownDiffs = new HashSet<>();

    // Methods that we do not need to test from the Super class
    acceptedKnownDiffs.add(Map.entry("unwrap", 1));
    acceptedKnownDiffs.add(Map.entry("isWrapperFor", 1));

    return acceptedKnownDiffs;
  }

  @Override
  public Map<Map.Entry<String, Integer>, Set<Object[]>> getFunctionToArgsMap() {
    Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap = new HashMap<>();

    for (Integer i : getColumnIndexList()) {
      putInMapForKey(functionToArgsMap, Map.entry("getColumnDisplaySize", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("isSearchable", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("isCurrency", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("isNullable", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("getScale", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("isReadOnly", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("getColumnClassName", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("isAutoIncrement", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("getPrecision", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("isWritable", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("getColumnLabel", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("getColumnName", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("isDefinitelyWritable", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("getColumnTypeName", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("getColumnType", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("getTableName", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("isSigned", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("getCatalogName", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("isCaseSensitive", 1), new Object[] {i});
      putInMapForKey(functionToArgsMap, Map.entry("getSchemaName", 1), new Object[] {i});
    }

    return functionToArgsMap;
  }

  /*
  The table used is same as the one in ResultSetTestParams
   */

  private static List<Integer> getColumnIndexList() {
    return IntStream.range(1, 19).boxed().collect(Collectors.toList());
  }
}
