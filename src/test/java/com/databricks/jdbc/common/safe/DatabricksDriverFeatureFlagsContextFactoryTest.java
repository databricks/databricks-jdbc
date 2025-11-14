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
  void testReferenceCountingWorks() {
    // Get instance three times for same workspace
    DatabricksDriverFeatureFlagsContext context1 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContext context2 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContext context3 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);

    assertSame(context1, context2);
    assertSame(context2, context3);

    // Remove twice - context should still exist
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext1);

    // Get again - should still return the same instance
    DatabricksDriverFeatureFlagsContext context4 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    assertSame(context1, context4);

    // Remove remaining references
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext1);
  }

  @Test
  void testRemoveInstanceWithNullContext() {
    // Should not throw exception
    assertDoesNotThrow(() -> DatabricksDriverFeatureFlagsContextFactory.removeInstance(null));
  }

  @Test
  void testContextPersistsUntilLastRemoval() {
    // Create multiple references
    DatabricksDriverFeatureFlagsContext context1 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContext context2 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);

    // Set a feature flag
    Map<String, String> flags = new HashMap<>();
    flags.put("test.flag", "true");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext1, flags);

    // Get the context again - should have the flag
    DatabricksDriverFeatureFlagsContext context3 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    assertTrue(context3.isFeatureEnabled("test.flag"));

    // Remove one reference - context should still exist
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext1);

    // Get again - should still have the flag
    DatabricksDriverFeatureFlagsContext context4 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    assertTrue(context4.isFeatureEnabled("test.flag"));

    // Clean up remaining references
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext1);
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
