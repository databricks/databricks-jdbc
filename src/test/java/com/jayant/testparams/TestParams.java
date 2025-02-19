package com.jayant.testparams;

import java.util.Map;
import java.util.Set;

public interface TestParams {
  public Set<Map.Entry<String, Integer>> getAcceptedKnownDiffs();

  public Map<Map.Entry<String, Integer>, Object[]> getFunctionToArgsMap();
}
