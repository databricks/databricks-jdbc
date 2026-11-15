package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.safe.DatabricksDriverFeatureFlagsContextFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class ApcRoutingTest {
  private static final String APC_FLAG =
      "databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc";
  private static final String WAREHOUSE_FLAG =
      "databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc";
  private static final String BASE =
      "jdbc:databricks://host.databricks.com:443/default;AuthMech=3;httpPath=";
  private static final String CLUSTER = "/sql/protocolv1/o/org-1/cluster-1";
  private static final String WAREHOUSE = "/sql/1.0/warehouses/warehouse-1";

  private DatabricksConnectionContext current;

  @AfterEach
  void cleanup() {
    if (current != null) {
      DatabricksDriverFeatureFlagsContextFactory.removeInstance(current);
    }
  }

  private DatabricksConnectionContext context(
      String path, String options, String apcFlag, String warehouseFlag) throws Exception {
    current =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(BASE + path + options, new Properties());
    Map<String, String> flags = new HashMap<>();
    if (apcFlag != null) {
      flags.put(APC_FLAG, apcFlag);
    }
    if (warehouseFlag != null) {
      flags.put(WAREHOUSE_FLAG, warehouseFlag);
    }
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(current, flags);
    return current;
  }

  @Test
  void apcFlagIsIndependentOfWarehouseFlag() throws Exception {
    assertEquals(DatabricksClientType.THRIFT, context(CLUSTER, "", null, "true").getClientType());
    cleanup();
    assertEquals(
        DatabricksClientType.THRIFT, context(CLUSTER, "", "false", "true").getClientType());
    cleanup();
    assertEquals(DatabricksClientType.SEA, context(CLUSTER, "", "true", "false").getClientType());
    assertEquals("apc_flag", current.getRoutingReason());
    cleanup();
    assertEquals(
        DatabricksClientType.THRIFT, context(WAREHOUSE, "", "true", "false").getClientType());
  }

  @Test
  void explicitProtocolAndCatalogPatternPrecedence() throws Exception {
    assertEquals(
        DatabricksClientType.THRIFT,
        context(CLUSTER, ";UseThriftClient=1", "true", null).getClientType());
    assertEquals("explicit_thrift", current.getRoutingReason());
    cleanup();
    assertEquals(
        DatabricksClientType.SEA,
        context(CLUSTER, ";UseThriftClient=0", "false", null).getClientType());
    cleanup();
    assertEquals(
        DatabricksClientType.THRIFT,
        context(CLUSTER, ";TreatMetadataCatalogNameAsPattern=1", "true", null).getClientType());
    cleanup();
    assertEquals(
        DatabricksClientType.SEA,
        context(CLUSTER, ";TreatMetadataCatalogNameAsPattern=1;UseThriftClient=0", "true", null)
            .getClientType());
  }

  @Test
  void legacyPathAndInlineMode() throws Exception {
    assertEquals(
        DatabricksClientType.THRIFT,
        context("cliservice", ";UseThriftClient=0", "true", null).getClientType());
    cleanup();
    assertEquals(
        DatabricksClientType.SEA,
        context(CLUSTER, ";EnableQueryResultDownload=0;UseBoundedSeaApi=0", "true", null)
            .getClientType());
    cleanup();
    assertEquals(
        DatabricksClientType.SEA,
        context(CLUSTER, ";EnableQueryResultDownload=0;UseBoundedSeaApi=1", "true", null)
            .getClientType());
  }

  @Test
  void arrowUnavailableOverridesExplicitSea() throws Exception {
    String oldName = System.getProperty("os.name");
    try {
      System.setProperty("os.name", "AIX");
      assertEquals(
          DatabricksClientType.THRIFT,
          context(CLUSTER, ";UseThriftClient=0", "true", null).getClientType());
      assertEquals("arrow_unavailable", current.getRoutingReason());
    } finally {
      if (oldName == null) {
        System.clearProperty("os.name");
      } else {
        System.setProperty("os.name", oldName);
      }
    }
  }

  @Test
  void failedApcFlagLookupFallsBackToThrift() throws Exception {
    current =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(BASE + CLUSTER, new Properties());
    try (MockedStatic<DatabricksDriverFeatureFlagsContextFactory> flags =
        Mockito.mockStatic(DatabricksDriverFeatureFlagsContextFactory.class)) {
      flags
          .when(() -> DatabricksDriverFeatureFlagsContextFactory.getInstance(current))
          .thenThrow(new IllegalStateException("flag service unavailable"));
      assertEquals(DatabricksClientType.THRIFT, current.getClientType());
      assertEquals("apc_flag_unavailable", current.getRoutingReason());
    }
  }
}
