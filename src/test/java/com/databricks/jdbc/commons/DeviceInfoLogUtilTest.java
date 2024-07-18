package com.databricks.jdbc.commons;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.commons.util.DeviceInfoLogUtil;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DeviceInfoLogUtilTest {
  @Mock IDatabricksConnectionContext context;
  @Mock ComputeResource computeResource;

  @Test
  public void testLogProperties() throws DatabricksSQLException {
    when(context.enableTelemetry()).thenReturn(true);
    when(context.getComputeResource()).thenReturn(computeResource);
    when(computeResource.getWorkspaceId()).thenReturn("workspaceId");
    assertDoesNotThrow(() -> DeviceInfoLogUtil.logProperties(context));
  }
}
