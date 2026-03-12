package com.databricks.jdbc.api.impl.streaming;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Unit tests for StreamingBatch state management and lifecycle. */
public class StreamingBatchTest {

  @Test
  void testInitialState() {
    StreamingBatch<String> batch = new StreamingBatch<>(0, 0, s -> {});

    assertEquals(StreamingBatch.Status.PENDING, batch.getStatus());
    assertEquals(0, batch.getBatchIndex());
    assertEquals(0, batch.getRowOffset());
    assertNull(batch.getData());
    assertFalse(batch.isReady());
  }

  @Test
  void testSetFetching() {
    StreamingBatch<String> batch = new StreamingBatch<>(1, 100, s -> {});

    batch.setFetching();

    assertEquals(StreamingBatch.Status.FETCHING, batch.getStatus());
    assertFalse(batch.isReady());
  }

  @Test
  void testSetData() {
    StreamingBatch<String> batch = new StreamingBatch<>(2, 200, s -> {});

    batch.setData("test_data", 50, true);

    assertEquals(StreamingBatch.Status.READY, batch.getStatus());
    assertTrue(batch.isReady());
    assertEquals("test_data", batch.getData());
    assertEquals(50, batch.getRowCount());
    assertTrue(batch.hasMoreRows());
  }

  @Test
  void testSetError() {
    StreamingBatch<String> batch = new StreamingBatch<>(3, 300, s -> {});
    Exception testError = new RuntimeException("Test error");

    batch.setError(testError);

    assertEquals(StreamingBatch.Status.ERROR, batch.getStatus());
    assertFalse(batch.isReady());
    assertEquals(testError, batch.getError());
  }

  @Test
  void testRelease() {
    AtomicBoolean releaseActionCalled = new AtomicBoolean(false);
    StreamingBatch<String> batch = new StreamingBatch<>(4, 400, s -> releaseActionCalled.set(true));

    batch.setData("to_be_released", 10, false);
    batch.release();

    assertEquals(StreamingBatch.Status.RELEASED, batch.getStatus());
    assertNull(batch.getData());
    assertTrue(releaseActionCalled.get());
  }

  @Test
  void testReleaseWithNullData() {
    AtomicBoolean releaseActionCalled = new AtomicBoolean(false);
    StreamingBatch<String> batch = new StreamingBatch<>(5, 500, s -> releaseActionCalled.set(true));

    // Release without setting data
    batch.release();

    assertEquals(StreamingBatch.Status.RELEASED, batch.getStatus());
    assertFalse(releaseActionCalled.get()); // Should not call release action for null data
  }

  @Test
  void testWaitUntilReadyCompletesWhenDataSet() throws Exception {
    StreamingBatch<String> batch = new StreamingBatch<>(6, 600, s -> {});

    // Set data in another thread
    Thread setter =
        new Thread(
            () -> {
              try {
                Thread.sleep(50);
                batch.setData("async_data", 5, false);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    setter.start();

    // Wait should complete successfully
    batch.waitUntilReady(5);

    assertTrue(batch.isReady());
    assertEquals("async_data", batch.getData());
  }

  @Test
  void testWaitUntilReadyThrowsOnError() {
    StreamingBatch<String> batch = new StreamingBatch<>(7, 700, s -> {});
    RuntimeException testError = new RuntimeException("Fetch failed");

    // Set error in another thread
    Thread errorSetter =
        new Thread(
            () -> {
              try {
                Thread.sleep(50);
                batch.setError(testError);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    errorSetter.start();

    // Wait should throw ExecutionException
    assertThrows(ExecutionException.class, () -> batch.waitUntilReady(5));
  }

  @Test
  void testWaitUntilReadyTimeout() {
    StreamingBatch<String> batch = new StreamingBatch<>(8, 800, s -> {});

    // Never set data - should timeout
    assertThrows(TimeoutException.class, () -> batch.waitUntilReady(1));
  }

  @Test
  void testStateTransitionFullLifecycle() {
    AtomicBoolean released = new AtomicBoolean(false);
    StreamingBatch<String> batch = new StreamingBatch<>(9, 900, s -> released.set(true));

    // PENDING
    assertEquals(StreamingBatch.Status.PENDING, batch.getStatus());

    // PENDING -> FETCHING
    batch.setFetching();
    assertEquals(StreamingBatch.Status.FETCHING, batch.getStatus());

    // FETCHING -> READY
    batch.setData("lifecycle_data", 25, false);
    assertEquals(StreamingBatch.Status.READY, batch.getStatus());

    // READY -> RELEASED
    batch.release();
    assertEquals(StreamingBatch.Status.RELEASED, batch.getStatus());
    assertTrue(released.get());
  }
}
