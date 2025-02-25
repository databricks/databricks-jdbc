package com.jayant.testparams;

import java.sql.ResultSet;
import java.util.*;

public class ParamUtils {
  static void putInMapForKey(
      Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap,
      Map.Entry<String, Integer> key,
      Object[] value) {
    functionToArgsMap.putIfAbsent(key, new HashSet<>());
    functionToArgsMap.get(key).add(value);
  }

  public static List<Integer> getAllFetchDirection() {
    return Arrays.asList(ResultSet.FETCH_FORWARD, ResultSet.FETCH_REVERSE, ResultSet.FETCH_UNKNOWN);
  }

  public static List<Integer> getAllConcurrencyCondition() {
    return Arrays.asList(ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE);
  }
}
