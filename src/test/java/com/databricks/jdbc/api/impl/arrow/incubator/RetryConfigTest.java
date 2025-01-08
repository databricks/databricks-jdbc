package com.databricks.jdbc.api.impl.arrow.incubator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class RetryConfigTest {

  @Test
  public void testDefaultValues() {
    RetryConfig config = new RetryConfig.Builder().build();
    assertEquals(5, config.maxAttempts);
    assertEquals(1000, config.baseDelayMs);
    assertEquals(5000, config.maxDelayMs);
  }

  @Test
  public void testCustomValues() {
    RetryConfig config =
        new RetryConfig.Builder().maxAttempts(3).baseDelayMs(2000).maxDelayMs(10000).build();

    assertEquals(3, config.maxAttempts);
    assertEquals(2000, config.baseDelayMs);
    assertEquals(10000, config.maxDelayMs);
  }

  @Test
  public void testBuilderChaining() {
    RetryConfig config =
        new RetryConfig.Builder()
            .maxAttempts(3)
            .baseDelayMs(2000)
            .maxDelayMs(10000)
            .maxAttempts(4) // Override previous value
            .build();

    assertEquals(4, config.maxAttempts);
  }
}
