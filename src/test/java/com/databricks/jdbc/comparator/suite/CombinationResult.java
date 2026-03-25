package com.databricks.jdbc.comparator.suite;

import java.util.List;

/** Result of comparing one argument combination for a DatabaseMetaData method. */
public class CombinationResult {
  final String argsLabel;
  final List<String> metadataDiffs;
  final List<String> dataDiffs;

  CombinationResult(String argsLabel, List<String> metadataDiffs, List<String> dataDiffs) {
    this.argsLabel = argsLabel;
    this.metadataDiffs = metadataDiffs;
    this.dataDiffs = dataDiffs;
  }
}
