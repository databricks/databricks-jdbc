package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class NetworkRetryUtilTest {
  @Test
  void zeroSocketTimeoutUsesDefaultRetryWindow() {
    assertTrue(NetworkRetryUtil.hasRetryTimeRemaining(TimeUnit.SECONDS.toNanos(899), 0, 0));
    assertFalse(NetworkRetryUtil.hasRetryTimeRemaining(TimeUnit.SECONDS.toNanos(900), 0, 0));
    assertTrue(NetworkRetryUtil.hasRetryTimeRemaining(TimeUnit.SECONDS.toNanos(897), 2000, 0));
    assertFalse(NetworkRetryUtil.hasRetryTimeRemaining(TimeUnit.SECONDS.toNanos(898), 2000, 0));
  }
}
