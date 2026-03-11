package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DeviceInfoLogUtilTest {
  @Mock IDatabricksConnectionContext context;

  @Test
  public void testLogProperties() {
    assertDoesNotThrow(() -> DeviceInfoLogUtil.logProperties());
  }
}
