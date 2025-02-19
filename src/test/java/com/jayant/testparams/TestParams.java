package com.jayant.testparams;

import java.util.Map;
import java.util.Set;

public interface TestParams {
  // Set(<methodName1, paramCount1>, <methodName2, paramCount2>, ...)
  Set<Map.Entry<String, Integer>> getAcceptedKnownDiffs();

  // Map (<methodName, paramCount>) -> Set(testParams1, testParams2, ...)
  Map<Map.Entry<String, Integer>, Set<Object[]>> getFunctionToArgsMap();
}
