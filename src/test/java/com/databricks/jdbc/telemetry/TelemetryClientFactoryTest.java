package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.common.safe.FeatureFlagTestUtil.enableFeatureFlagForTesting;
import static com.databricks.jdbc.telemetry.TelemetryHelper.TELEMETRY_FEATURE_FLAG_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.TelemetryAuthHelper;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.sdk.core.DatabricksConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TelemetryClientFactoryTest {
  private static final String JDBC_URL_1 =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;UserAgentEntry=MyApp;";
  private static final String JDBC_URL_2 =
      "jdbc:databricks://adb-20.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/ghgjhgj;UserAgentEntry=MyApp;forceEnableTelemetry=1";

  @Mock ClientConfigurator clientConfigurator;
  @Mock DatabricksConfig databricksConfig;

  @BeforeEach
  public void setUp() {
    // Reset the singleton to ensure clean state between tests
    TelemetryClientFactory.getInstance().reset();
  }

  @Test
  public void testGetNoOpTelemetryClient() throws Exception {
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL_1, new Properties());
    TelemetryClientFactory.getInstance().closeTelemetryClient(context);
    ITelemetryClient telemetryClient =
        TelemetryClientFactory.getInstance().getTelemetryClient(context);
    assertInstanceOf(NoopTelemetryClient.class, telemetryClient);
    assertEquals(0, TelemetryClientFactory.getInstance().telemetryClientHolders.size());
    assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());
  }

  @Test
  public void testGetAuthenticatedTelemetryClient() throws Exception {
    Properties properties = new Properties();
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL_2, properties);
    when(clientConfigurator.getDatabricksConfig()).thenReturn(databricksConfig);
    setupMocksForTelemetryClient(context);
    ITelemetryClient telemetryClient =
        TelemetryClientFactory.getInstance().getTelemetryClient(context);
    assertInstanceOf(TelemetryClient.class, telemetryClient);
    assertEquals(1, TelemetryClientFactory.getInstance().telemetryClientHolders.size());
    assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());
    TelemetryClientFactory.getInstance().closeTelemetryClient(context);
    assertEquals(0, TelemetryClientFactory.getInstance().telemetryClientHolders.size());
    assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());
    TelemetryClientFactory.getInstance().closeTelemetryClient(context);
  }

  @Test
  public void testGetNoOpTelemetryClientWhenDatabricksConfigIsNull() throws Exception {
    // Create a context with telemetry enabled but no DatabricksConfig
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL_2, new Properties());
    setupMocksForTelemetryClient(context);

    // Mock TelemetryHelper to return null for DatabricksConfig
    try (MockedStatic<TelemetryHelper> mockedStatic = mockStatic(TelemetryHelper.class)) {
      mockedStatic.when(() -> TelemetryHelper.getDatabricksConfigSafely(context)).thenReturn(null);
      mockedStatic.when(() -> TelemetryHelper.keyOf(any())).thenCallRealMethod();
      ITelemetryClient telemetryClient =
          TelemetryClientFactory.getInstance().getTelemetryClient(context);

      assertInstanceOf(NoopTelemetryClient.class, telemetryClient);
      assertEquals(0, TelemetryClientFactory.getInstance().telemetryClientHolders.size());
      assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());
      TelemetryClientFactory.getInstance().closeTelemetryClient(context);
    }
  }

  // UUID Tracking Tests

  @ParameterizedTest
  @ValueSource(ints = {1, 5})
  void testMultipleConnectionsToSameHostShareClient(int connectionCount) throws Exception {
    String host = "shared-host.databricks.net";
    IDatabricksConnectionContext[] contexts = new IDatabricksConnectionContext[connectionCount];

    try (MockedStatic<TelemetryHelper> mockedStatic = mockStatic(TelemetryHelper.class)) {
      setupTelemetryHelperMock(mockedStatic);

      // Create multiple connections to same host
      for (int i = 0; i < connectionCount; i++) {
        contexts[i] = createMockContext("uuid-" + i, host, true);
      }

      // Get clients - all should return same instance
      ITelemetryClient firstClient =
          TelemetryClientFactory.getInstance().getTelemetryClient(contexts[0]);
      for (int i = 1; i < connectionCount; i++) {
        ITelemetryClient client =
            TelemetryClientFactory.getInstance().getTelemetryClient(contexts[i]);
        assertSame(firstClient, client, "Connection " + i + " should share same client");
      }

      // Verify single holder with all UUIDs
      assertEquals(1, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());

      // Close connections one by one - holder should remain until last connection closes
      for (int i = 0; i < connectionCount - 1; i++) {
        TelemetryClientFactory.getInstance().closeTelemetryClient(contexts[i]);
        assertEquals(
            1,
            TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size(),
            "Client should remain after closing connection " + i);
      }

      // Close last connection - client should be removed
      TelemetryClientFactory.getInstance().closeTelemetryClient(contexts[connectionCount - 1]);
      assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());
    }
  }

  private static Stream<Arguments> provideAuthenticationScenarios() {
    return Stream.of(Arguments.of("authenticated", true), Arguments.of("no-auth", false));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("provideAuthenticationScenarios")
  void testDifferentHostsGetDifferentClients(String scenario, boolean authenticated)
      throws Exception {
    try (MockedStatic<TelemetryHelper> mockedStatic = mockStatic(TelemetryHelper.class)) {
      setupTelemetryHelperMock(mockedStatic);
      if (authenticated) {
        mockedStatic
            .when(() -> TelemetryHelper.getDatabricksConfigSafely(any()))
            .thenReturn(databricksConfig);
      }

      IDatabricksConnectionContext ctx1 =
          createMockContext("uuid-1", "host1.databricks.net", authenticated);
      IDatabricksConnectionContext ctx2 =
          createMockContext("uuid-2", "host2.databricks.net", authenticated);

      ITelemetryClient client1 = TelemetryClientFactory.getInstance().getTelemetryClient(ctx1);
      ITelemetryClient client2 = TelemetryClientFactory.getInstance().getTelemetryClient(ctx2);

      assertNotSame(client1, client2, "Different hosts should get different clients");

      int holderCount =
          authenticated
              ? TelemetryClientFactory.getInstance().telemetryClientHolders.size()
              : TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size();
      assertEquals(2, holderCount);

      // Close first connection
      TelemetryClientFactory.getInstance().closeTelemetryClient(ctx1);
      holderCount =
          authenticated
              ? TelemetryClientFactory.getInstance().telemetryClientHolders.size()
              : TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size();
      assertEquals(1, holderCount, "First client should be closed");

      // Close second connection
      TelemetryClientFactory.getInstance().closeTelemetryClient(ctx2);
      holderCount =
          authenticated
              ? TelemetryClientFactory.getInstance().telemetryClientHolders.size()
              : TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size();
      assertEquals(0, holderCount, "All clients should be closed");
    }
  }

  @Test
  void testConnectionParamCacheCleanedOnClose() throws Exception {
    String uuid = "test-uuid-123";
    String host = "test-host.databricks.net";

    try (MockedStatic<TelemetryHelper> mockedStatic = mockStatic(TelemetryHelper.class)) {
      setupTelemetryHelperMock(mockedStatic);

      IDatabricksConnectionContext ctx = createMockContext(uuid, host, false);
      TelemetryClientFactory.getInstance().getTelemetryClient(ctx);

      // Close connection and verify cleanup is called
      TelemetryClientFactory.getInstance().closeTelemetryClient(ctx);

      // Verify removeConnectionParameters was called
      mockedStatic.verify(() -> TelemetryHelper.removeConnectionParameters(uuid), times(1));
    }
  }

  @Test
  void testUUIDTrackingWithMixedConnectionLifecycles() throws Exception {
    String host = "shared-host.databricks.net";

    try (MockedStatic<TelemetryHelper> mockedStatic = mockStatic(TelemetryHelper.class)) {
      setupTelemetryHelperMock(mockedStatic);

      IDatabricksConnectionContext conn1 = createMockContext("uuid-1", host, false);
      IDatabricksConnectionContext conn2 = createMockContext("uuid-2", host, false);
      IDatabricksConnectionContext conn3 = createMockContext("uuid-3", host, false);

      // Open connections in order: 1, 2, 3
      ITelemetryClient client1 = TelemetryClientFactory.getInstance().getTelemetryClient(conn1);
      ITelemetryClient client2 = TelemetryClientFactory.getInstance().getTelemetryClient(conn2);
      ITelemetryClient client3 = TelemetryClientFactory.getInstance().getTelemetryClient(conn3);

      // All should share same client
      assertSame(client1, client2);
      assertSame(client2, client3);
      assertEquals(1, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());

      // Close middle connection - client should remain
      TelemetryClientFactory.getInstance().closeTelemetryClient(conn2);
      assertEquals(1, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());

      // Close first connection - client should still remain
      TelemetryClientFactory.getInstance().closeTelemetryClient(conn1);
      assertEquals(1, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());

      // Close last connection - NOW client should be removed
      TelemetryClientFactory.getInstance().closeTelemetryClient(conn3);
      assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());
    }
  }

  @Test
  void testDoubleCloseDoesNotCauseErrors() throws Exception {
    String host = "test-host.databricks.net";

    try (MockedStatic<TelemetryHelper> mockedStatic = mockStatic(TelemetryHelper.class)) {
      setupTelemetryHelperMock(mockedStatic);

      IDatabricksConnectionContext ctx = createMockContext("uuid-1", host, false);
      TelemetryClientFactory.getInstance().getTelemetryClient(ctx);

      // Close twice - should not throw
      assertDoesNotThrow(() -> TelemetryClientFactory.getInstance().closeTelemetryClient(ctx));
      assertDoesNotThrow(() -> TelemetryClientFactory.getInstance().closeTelemetryClient(ctx));

      assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());
    }
  }

  @Test
  void testSameConnectionCallingGetClientMultipleTimes() throws Exception {
    String host = "test-host.databricks.net";

    try (MockedStatic<TelemetryHelper> mockedStatic = mockStatic(TelemetryHelper.class)) {
      setupTelemetryHelperMock(mockedStatic);

      IDatabricksConnectionContext conn = createMockContext("uuid-1", host, false);

      // Call getTelemetryClient multiple times with same connection (e.g., retry logic)
      TelemetryClientFactory.getInstance().getTelemetryClient(conn);
      TelemetryClientFactory.getInstance().getTelemetryClient(conn);
      TelemetryClientFactory.getInstance().getTelemetryClient(conn);

      assertEquals(1, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());

      // Single close should remove client - UUID set deduplicates, old refCount would leak
      TelemetryClientFactory.getInstance().closeTelemetryClient(conn);
      assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());
    }
  }

  // Helper methods

  private IDatabricksConnectionContext createMockContext(
      String uuid, String host, boolean authenticated) {
    IDatabricksConnectionContext ctx = mock(IDatabricksConnectionContext.class);
    lenient().when(ctx.getConnectionUuid()).thenReturn(uuid);
    lenient().when(ctx.getHost()).thenReturn(host);
    lenient().when(ctx.isTelemetryEnabled()).thenReturn(true);
    lenient().when(ctx.getTelemetryBatchSize()).thenReturn(10);
    lenient().when(ctx.getTelemetryFlushIntervalInMilliseconds()).thenReturn(5000);
    if (authenticated) {
      lenient().when(clientConfigurator.getDatabricksConfig()).thenReturn(databricksConfig);
    }
    return ctx;
  }

  private void setupTelemetryHelperMock(MockedStatic<TelemetryHelper> mockedStatic) {
    mockedStatic.when(() -> TelemetryHelper.keyOf(any())).thenCallRealMethod();
    mockedStatic.when(() -> TelemetryHelper.getDatabricksConfigSafely(any())).thenReturn(null);
    mockedStatic
        .when(() -> TelemetryHelper.removeConnectionParameters(anyString()))
        .thenAnswer(invocation -> null);
    mockedStatic
        .when(() -> TelemetryHelper.isTelemetryAllowedForConnection(any()))
        .thenReturn(true);
  }

  private void setupMocksForTelemetryClient(IDatabricksConnectionContext context) {
    TelemetryClientFactory.getInstance().closeTelemetryClient(context);
    TelemetryAuthHelper.setupAuthMocks(context, clientConfigurator);
    Map<String, String> featureFlagMap = new HashMap<>();
    featureFlagMap.put(TELEMETRY_FEATURE_FLAG_NAME, "true");
    enableFeatureFlagForTesting(context, featureFlagMap);
  }
}
