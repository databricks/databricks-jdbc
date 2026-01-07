package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DriverUtilTest {

  @Test
  public void testGetDriverVersion() {
    String version = DriverUtil.getDriverVersion();
    assertNotNull(version);
    assertEquals("3.1.1", version);
  }

  @Test
  public void testGetDriverVersionWithoutOSSSuffix() {
    String version = DriverUtil.getDriverVersionWithoutOSSSuffix();
    assertNotNull(version);
    assertEquals("3.1.1", version);
  }

  @Test
  public void testGetDriverName() {
    String name = DriverUtil.getDriverName();
    assertNotNull(name);
    assertEquals("oss-jdbc", name);
  }

  @Test
  public void testGetDriverMajorVersion() {
    int majorVersion = DriverUtil.getDriverMajorVersion();
    assertEquals(3, majorVersion);
  }

  @Test
  public void testGetDriverMinorVersion() {
    int minorVersion = DriverUtil.getDriverMinorVersion();
    assertEquals(1, minorVersion);
  }

  @Test
  public void testGetJDBCMajorVersion() {
    int majorVersion = DriverUtil.getJDBCMajorVersion();
    assertEquals(4, majorVersion);
  }

  @Test
  public void testGetJDBCMinorVersion() {
    int minorVersion = DriverUtil.getJDBCMinorVersion();
    assertEquals(3, minorVersion);
  }

  @Test
  public void testIsRunningAgainstFake() {
    // Test default behavior (should be false when property is not set)
    boolean result = DriverUtil.isRunningAgainstFake();
    assertEquals(false, result);
  }
}
