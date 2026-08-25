package com.databricks.jdbc.comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksSession;
import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.ApcSeaPrototype;
import com.databricks.jdbc.common.SeaCircuitBreakerManager;
import com.databricks.jdbc.comparator.config.ConnectionManager;
import com.databricks.jdbc.comparator.setup.WorkspaceSetup;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class JDBCDriverComparisonLifecycleTest {
  private static final String BASE_URL =
      "jdbc:databricks://workspace.example.com:443/default;ssl=1;authMech=3;"
          + "httpPath=/sql/protocolv1/o/123/cluster-id;UseThriftClient=";

  @Test
  void disabledWorkspaceSetupOpensOnlyTheSelectedApcRoutePair() throws Exception {
    Map<String, String> previousProperties = new LinkedHashMap<>();
    setProperty(previousProperties, "CONNECTION_CONFIG", "DEFAULT_PARAMS");
    setProperty(previousProperties, "SUITES_RUN_ONLY", "APC_CORE,APC_EXTENDED");
    setProperty(previousProperties, "WORKSPACE_SETUP", "");
    setProperty(previousProperties, ApcSeaPrototype.ENABLED_PROPERTY, "true");

    PrintStream originalOut = System.out;
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    SeaCircuitBreakerManager.reset();
    try {
      System.setOut(new PrintStream(output));
      ConnectionManager manager = mock(ConnectionManager.class);
      Connection left = mock(Connection.class);
      Connection right = mock(Connection.class);
      List<String> openedUrls = new ArrayList<>();
      List<DatabricksSession> openedSessions = new ArrayList<>();
      when(manager.getConnection(anyString()))
          .thenAnswer(
              invocation -> {
                String url = invocation.getArgument(0);
                openedUrls.add(url);
                IDatabricksConnectionContext context =
                    DatabricksConnectionContext.parse(url, new Properties());
                IDatabricksClient client = mock(IDatabricksClient.class);
                when(client.createSession(any(), any(), any(), any()))
                    .thenReturn(
                        ImmutableSessionInfo.builder()
                            .sessionId("session-" + openedUrls.size())
                            .computeResource(context.getComputeResource())
                            .build());
                DatabricksSession session = new DatabricksSession(context, client);
                session.open();
                openedSessions.add(session);
                return openedUrls.size() == 1 ? left : right;
              });

      WorkspaceSetup.run(() -> manager.getConnection("jdbc:must-not-open"));
      long testCount =
          JDBCDriverComparisonTest.buildTestArguments(manager, BASE_URL + "1", BASE_URL + "0")
              .count();

      assertEquals(10, testCount);
      assertEquals(2, openedUrls.size());
      assertTrue(openedUrls.get(0).contains("UseThriftClient=1"));
      assertTrue(openedUrls.get(1).contains("UseThriftClient=0"));
      assertTrue(openedUrls.stream().allMatch(url -> url.contains("EnableHeartbeat=0")));
      assertEquals(2, openedSessions.size());
      assertTrue(openedSessions.stream().allMatch(DatabricksSession::isOpen));
      verify(manager, times(2)).getConnection(anyString());
      assertEquals(
          List.of(
              "JDBC_APC_ROUTE route=THRIFT compute=APC no_fallback=false",
              "JDBC_APC_ROUTE route=SEA compute=APC no_fallback=true"),
          output.toString().lines().collect(Collectors.toList()));
      for (DatabricksSession session : openedSessions) {
        session.close();
      }
    } finally {
      System.setOut(originalOut);
      SeaCircuitBreakerManager.reset();
      restoreProperties(previousProperties);
    }
  }

  private static void setProperty(Map<String, String> previous, String key, String value) {
    previous.put(key, System.getProperty(key));
    System.setProperty(key, value);
  }

  private static void restoreProperties(Map<String, String> previous) {
    previous.forEach(
        (key, value) -> {
          if (value == null) {
            System.clearProperty(key);
          } else {
            System.setProperty(key, value);
          }
        });
  }
}
