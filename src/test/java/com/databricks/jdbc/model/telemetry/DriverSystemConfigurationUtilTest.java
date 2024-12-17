package com.databricks.jdbc.model.telemetry;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DriverSystemConfigurationUtilTest {
  @Test
  public void testGetSystemProperties() {
    assertDoesNotThrow(() -> DriverSystemConfiguration.getInstance().toString());
  }
}
