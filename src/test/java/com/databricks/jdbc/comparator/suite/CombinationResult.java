package com.databricks.jdbc.comparator.suite;

import java.util.Collections;
import java.util.List;

/** Result of comparing one argument combination for a DatabaseMetaData method. */
public class CombinationResult {
  public final String argsLabel;
  public final List<String> metadataDiffs;
  public final List<String> dataDiffs;
  public final boolean skipped;
  public final String skipReason;

  CombinationResult(String argsLabel, List<String> metadataDiffs, List<String> dataDiffs) {
    this.argsLabel = argsLabel;
    this.metadataDiffs = metadataDiffs;
    this.dataDiffs = dataDiffs;
    this.skipped = false;
    this.skipReason = null;
  }

  private CombinationResult(String argsLabel, String skipReason) {
    this.argsLabel = argsLabel;
    this.metadataDiffs = Collections.emptyList();
    this.dataDiffs = Collections.emptyList();
    this.skipped = true;
    this.skipReason = skipReason;
  }

  static CombinationResult skipped(String argsLabel) {
    return new CombinationResult(argsLabel, (String) null);
  }

  static CombinationResult skipped(String argsLabel, String reason) {
    return new CombinationResult(argsLabel, reason);
  }
}
