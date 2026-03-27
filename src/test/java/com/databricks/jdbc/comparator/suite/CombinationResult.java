package com.databricks.jdbc.comparator.suite;

import java.util.Collections;
import java.util.List;

/** Result of comparing one argument combination for a DatabaseMetaData method. */
public class CombinationResult {
  final String argsLabel;
  final List<String> metadataDiffs;
  final List<String> dataDiffs;
  final boolean skipped;

  CombinationResult(String argsLabel, List<String> metadataDiffs, List<String> dataDiffs) {
    this.argsLabel = argsLabel;
    this.metadataDiffs = metadataDiffs;
    this.dataDiffs = dataDiffs;
    this.skipped = false;
  }

  private CombinationResult(String argsLabel, boolean skipped) {
    this.argsLabel = argsLabel;
    this.metadataDiffs = Collections.emptyList();
    this.dataDiffs = Collections.emptyList();
    this.skipped = skipped;
  }

  static CombinationResult skipped(String argsLabel) {
    return new CombinationResult(argsLabel, true);
  }
}
