package com.databricks.jdbc.comparator.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ConnectionConfigTest {

  @Test
  void apcCoreDoesNotChangeTheDefaultComparatorRun() {
    String original = System.getProperty("SUITES_RUN_ONLY");
    try {
      System.clearProperty("SUITES_RUN_ONLY");
      assertFalse(
          ConnectionConfig.DEFAULT_PARAMS.getApplicableSuites().contains(TestSuite.APC_CORE));
      assertFalse(
          ConnectionConfig.DEFAULT_PARAMS.buildUrl("jdbc:test").contains("EnableHeartbeat"));

      System.setProperty("SUITES_RUN_ONLY", "STATEMENT_SELECT,APC_CORE");
      assertTrue(
          ConnectionConfig.DEFAULT_PARAMS.getApplicableSuites().contains(TestSuite.APC_CORE));
      assertTrue(
          ConnectionConfig.DEFAULT_PARAMS.buildUrl("jdbc:test").contains("EnableHeartbeat=0"));
      assertFalse(
          ConnectionConfig.COMPRESSION_DISABLED.getApplicableSuites().contains(TestSuite.APC_CORE));
    } finally {
      if (original == null) {
        System.clearProperty("SUITES_RUN_ONLY");
      } else {
        System.setProperty("SUITES_RUN_ONLY", original);
      }
    }
  }
}
