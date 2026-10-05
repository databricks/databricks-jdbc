package com.databricks.jdbc.common.safe;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.Warehouse;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DatabricksDriverFeatureFlagsContextFactoryTest {

  @Mock(lenient = true)
  private IDatabricksConnectionContext connectionContext1;

  @Mock(lenient = true)
  private IDatabricksConnectionContext connectionContext2;

  @Mock(lenient = true)
  private IDatabricksComputeResource computeResource1;

  @Mock(lenient = true)
  private IDatabricksComputeResource computeResource2;

  private static final String WORKSPACE_ID_1 = "workspace1";
  private static final String WORKSPACE_ID_2 = "workspace2";

  @BeforeEach
  void setUp() {
    // Set up compute resources with different workspace IDs
    when(connectionContext1.getComputeResource()).thenReturn(computeResource1);
    when(connectionContext2.getComputeResource()).thenReturn(computeResource2);
    when(computeResource1.getWorkspaceId()).thenReturn(WORKSPACE_ID_1);
    when(computeResource2.getWorkspaceId()).thenReturn(WORKSPACE_ID_2);
    when(connectionContext1.getHost()).thenReturn("host1.databricks.com");
    when(connectionContext2.getHost()).thenReturn("host2.databricks.com");
    when(connectionContext1.getHostForOAuth()).thenReturn("host1.databricks.com");
    when(connectionContext2.getHostForOAuth()).thenReturn("host2.databricks.com");
    when(connectionContext1.getConnectionUuid()).thenReturn("uuid-1");
    when(connectionContext2.getConnectionUuid()).thenReturn("uuid-2");
  }

  @AfterEach
  void tearDown() {
    // Clean up any contexts created during tests
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext2);
  }

  @Test
  void testGetInstanceReturnsSameContextForSameWorkspace() {
    DatabricksDriverFeatureFlagsContext context1 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContext context2 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);

    assertSame(context1, context2);
  }

  @Test
  void testGetInstanceCreatesDifferentContextForDifferentWorkspace() {
    DatabricksDriverFeatureFlagsContext context1 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContext context2 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext2);

    assertNotSame(context1, context2);
  }

  @Test
  void testMultipleCallsWithSameConnectionAreIdempotent() {
    // Multiple getInstance calls with the same connection UUID are idempotent
    DatabricksDriverFeatureFlagsContext context1 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);

    assertSame(
        context1, DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1));

    // A single removeInstance is sufficient to clean up one connection
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext1);

    // Context is gone; a new one would be created on next getInstance
    DatabricksDriverFeatureFlagsContext newContext =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    // New context was created because the old one was shut down
    // (context1 was already shut down by the single removeInstance above)
    assertNotNull(newContext);
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext1);
  }

  @Test
  void testMultipleConnectionsRequireAllToClose() {
    IDatabricksConnectionContext connA =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    IDatabricksConnectionContext connB =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());

    lenient().when(connA.getHost()).thenReturn("shared.databricks.com");
    lenient().when(connB.getHost()).thenReturn("shared.databricks.com");
    lenient().when(connA.getHostForOAuth()).thenReturn("shared.databricks.com");
    lenient().when(connB.getHostForOAuth()).thenReturn("shared.databricks.com");
    lenient().when(connA.getConnectionUuid()).thenReturn("uuid-connA");
    lenient().when(connB.getConnectionUuid()).thenReturn("uuid-connB");

    // Both connections share the same context
    DatabricksDriverFeatureFlagsContext ctxA =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connA);
    DatabricksDriverFeatureFlagsContext ctxB =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connB);
    assertSame(ctxA, ctxB);

    // Closing one connection keeps the context alive
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connA);
    DatabricksDriverFeatureFlagsContext ctxStillAlive =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connB);
    assertSame(ctxA, ctxStillAlive);

    // Closing the last connection removes the context
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connB);
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connB); // second remove is no-op
  }

  @Test
  void testConnectionContextUpdatedWhenOwnerConnectionCloses() {
    // This tests the fix for the HTTP client leak:
    // when the connection whose context is stored in the feature flags context closes,
    // the stored context should be updated to another active connection.
    IDatabricksConnectionContext connA =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    IDatabricksConnectionContext connB =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());

    lenient().when(connA.getHost()).thenReturn("shared2.databricks.com");
    lenient().when(connB.getHost()).thenReturn("shared2.databricks.com");
    lenient().when(connA.getHostForOAuth()).thenReturn("shared2.databricks.com");
    lenient().when(connB.getHostForOAuth()).thenReturn("shared2.databricks.com");
    lenient().when(connA.getConnectionUuid()).thenReturn("uuid-ownerA");
    lenient().when(connB.getConnectionUuid()).thenReturn("uuid-secondB");

    // connA creates the context (becomes the "owner")
    DatabricksDriverFeatureFlagsContext ctx =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connA);
    DatabricksDriverFeatureFlagsContextFactory.getInstance(connB);

    assertEquals("uuid-ownerA", ctx.getConnectionContext().getConnectionUuid());

    // When connA closes, the stored context should be updated to connB
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connA);
    assertEquals("uuid-secondB", ctx.getConnectionContext().getConnectionUuid());

    // Clean up
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connB);
  }

  @Test
  void testRemoveInstanceWithNullContext() {
    // Should not throw exception
    assertDoesNotThrow(() -> DatabricksDriverFeatureFlagsContextFactory.removeInstance(null));
  }

  @Test
  void testContextPersistsUntilLastRemoval() {
    // Create two distinct connections to the same host
    IDatabricksConnectionContext connX =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    IDatabricksConnectionContext connY =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    lenient().when(connX.getHost()).thenReturn("host1.databricks.com");
    lenient().when(connY.getHost()).thenReturn("host1.databricks.com");
    lenient().when(connX.getHostForOAuth()).thenReturn("host1.databricks.com");
    lenient().when(connY.getHostForOAuth()).thenReturn("host1.databricks.com");
    lenient().when(connX.getConnectionUuid()).thenReturn("uuid-X");
    lenient().when(connY.getConnectionUuid()).thenReturn("uuid-Y");

    // Set a feature flag
    Map<String, String> flags = new HashMap<>();
    flags.put("test.flag", "true");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connX, flags);

    DatabricksDriverFeatureFlagsContextFactory.getInstance(connX);
    DatabricksDriverFeatureFlagsContextFactory.getInstance(connY);

    DatabricksDriverFeatureFlagsContext context =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connX);
    assertTrue(context.isFeatureEnabled("test.flag"));

    // Remove connX — context should still exist because connY is still open
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connX);

    DatabricksDriverFeatureFlagsContext contextAfterX =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connY);
    assertTrue(contextAfterX.isFeatureEnabled("test.flag"));

    // Clean up
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connY);
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connY);
  }

  @Test
  void testSetFeatureFlagsContextWorks() {
    Map<String, String> flags = new HashMap<>();
    flags.put("feature1", "true");
    flags.put("feature2", "false");

    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext1, flags);

    DatabricksDriverFeatureFlagsContext context =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);

    assertTrue(context.isFeatureEnabled("feature1"));
    assertFalse(context.isFeatureEnabled("feature2"));
  }

  @Test
  void testMultipleConnectionsToSameWorkspaceShareFlags() {
    // Create two connection contexts with same workspace ID
    IDatabricksConnectionContext conn1 =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    IDatabricksConnectionContext conn2 =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    IDatabricksComputeResource resource = new Warehouse(WORKSPACE_ID_1);

    lenient().when(conn1.getComputeResource()).thenReturn(resource);
    lenient().when(conn2.getComputeResource()).thenReturn(resource);
    lenient().when(conn1.getHost()).thenReturn("host.databricks.com");
    lenient().when(conn2.getHost()).thenReturn("host.databricks.com");
    lenient().when(conn1.getHostForOAuth()).thenReturn("host.databricks.com");
    lenient().when(conn2.getHostForOAuth()).thenReturn("host.databricks.com");
    lenient().when(conn1.getConnectionUuid()).thenReturn("uuid-conn1");
    lenient().when(conn2.getConnectionUuid()).thenReturn("uuid-conn2");

    // Set flags for first connection
    Map<String, String> flags = new HashMap<>();
    flags.put("shared.flag", "true");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(conn1, flags);

    // Get context for second connection - should share the same flags
    DatabricksDriverFeatureFlagsContext context1 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(conn1);
    DatabricksDriverFeatureFlagsContext context2 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(conn2);

    assertSame(context1, context2);
    assertTrue(context2.isFeatureEnabled("shared.flag"));

    // Clean up
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(conn1);
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(conn2);
  }

  @Test
  void testDifferentWorkspacesHaveIsolatedFlags() {
    // Set different flags for each workspace
    Map<String, String> flags1 = new HashMap<>();
    flags1.put("workspace1.flag", "true");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext1, flags1);

    Map<String, String> flags2 = new HashMap<>();
    flags2.put("workspace2.flag", "true");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext2, flags2);

    // Get contexts
    DatabricksDriverFeatureFlagsContext context1 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContext context2 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext2);

    // Verify flags are isolated
    assertTrue(context1.isFeatureEnabled("workspace1.flag"));
    assertFalse(context1.isFeatureEnabled("workspace2.flag"));

    assertFalse(context2.isFeatureEnabled("workspace1.flag"));
    assertTrue(context2.isFeatureEnabled("workspace2.flag"));
  }
}
