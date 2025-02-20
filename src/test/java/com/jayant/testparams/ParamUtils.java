package com.jayant.testparams;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ParamUtils {
  static void putInMapForKey(
      Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap,
      Map.Entry<String, Integer> key,
      Object[] value) {
    functionToArgsMap.putIfAbsent(key, new HashSet<>());
    functionToArgsMap.get(key).add(value);
  }
}
