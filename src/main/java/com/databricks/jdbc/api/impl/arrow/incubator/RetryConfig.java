package com.databricks.jdbc.api.impl.arrow.incubator;

/** Configuration for retry behavior with exponential backoff. */
class RetryConfig {
  final int maxAttempts;
  final long baseDelayMs;
  final long maxDelayMs;

  private RetryConfig(Builder builder) {
    this.maxAttempts = builder.maxAttempts;
    this.baseDelayMs = builder.baseDelayMs;
    this.maxDelayMs = builder.maxDelayMs;
  }

  static class Builder {
    private int maxAttempts = 5;
    private long baseDelayMs = 1000;
    private long maxDelayMs = 5000;

    Builder maxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    Builder baseDelayMs(long baseDelayMs) {
      this.baseDelayMs = baseDelayMs;
      return this;
    }

    Builder maxDelayMs(long maxDelayMs) {
      this.maxDelayMs = maxDelayMs;
      return this;
    }

    RetryConfig build() {
      return new RetryConfig(this);
    }
  }
}
