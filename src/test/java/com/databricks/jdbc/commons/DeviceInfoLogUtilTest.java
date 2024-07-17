package com.databricks.jdbc.commons;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.commons.util.DeviceInfoLogUtil;
import org.junit.jupiter.api.Test;

public class DeviceInfoLogUtilTest {
  @Test
  public void testLogProperties() {
    assertDoesNotThrow(DeviceInfoLogUtil::logProperties);
  }
}
