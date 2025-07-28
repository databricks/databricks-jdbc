package com.databricks.jdbc.api.impl.arrow.incubator;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.message.BasicHttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StreamingResponseConsumerTest {
  @Mock private ArrowResultChunkV2 mockChunk;
  private StreamingResponseConsumer consumer;

  @BeforeEach
  void setUp() {
    consumer = new StreamingResponseConsumer(mockChunk);
  }

  @Test
  void testStart_SuccessfulResponse() {
    BasicHttpResponse response = new BasicHttpResponse(HttpStatus.SC_OK);
    ContentType contentType = ContentType.APPLICATION_OCTET_STREAM;

    assertDoesNotThrow(() -> consumer.start(response, contentType));
  }

  @Test
  void testStart_NonSuccessfulResponse() {
    BasicHttpResponse response = new BasicHttpResponse(HttpStatus.SC_BAD_REQUEST);
    ContentType contentType = ContentType.APPLICATION_OCTET_STREAM;

    assertThrows(HttpException.class, () -> consumer.start(response, contentType));
  }

  @Test
  void testCapacityIncrement() {
    int expectedIncrement = 1024 * 1024; // 1MB

    assertEquals(expectedIncrement, consumer.capacityIncrement());
  }

  @Test
  void testData_SingleChunk() throws IOException {
    byte[] testData = "test data".getBytes();
    ByteBuffer buffer = ByteBuffer.wrap(testData);

    consumer.data(buffer, true);
    byte[] result = consumer.buildResult();

    assertArrayEquals(testData, result);
  }

  @Test
  void testData_MultipleChunks() throws IOException {
    byte[] chunk1 = "first chunk".getBytes();
    byte[] chunk2 = "second chunk".getBytes();

    consumer.data(ByteBuffer.wrap(chunk1), false);
    consumer.data(ByteBuffer.wrap(chunk2), true);
    byte[] result = consumer.buildResult();

    byte[] expectedData = new byte[chunk1.length + chunk2.length];
    System.arraycopy(chunk1, 0, expectedData, 0, chunk1.length);
    System.arraycopy(chunk2, 0, expectedData, chunk1.length, chunk2.length);

    assertArrayEquals(expectedData, result);
  }

  @Test
  void testData_EmptyChunk() throws IOException {
    ByteBuffer emptyBuffer = ByteBuffer.wrap(new byte[0]);

    consumer.data(emptyBuffer, true);
    byte[] result = consumer.buildResult();

    assertEquals(0, result.length);
  }

  @Test
  void testData_LargeChunk() throws IOException {
    int largeSize = 5 * 1024 * 1024; // 5MB
    byte[] largeData = new byte[largeSize];
    // Fill with some pattern
    for (int i = 0; i < largeSize; i++) {
      largeData[i] = (byte) (i % 256);
    }

    consumer.data(ByteBuffer.wrap(largeData), true);
    byte[] result = consumer.buildResult();

    assertArrayEquals(largeData, result);
  }

  @Test
  void testFailed() {
    Exception testException = new IOException("Test exception");

    consumer.failed(testException);

    assertEquals(0, consumer.buildResult().length);
  }

  @Test
  void testReleaseResources() throws IOException {
    byte[] testData = "test data".getBytes();
    consumer.data(ByteBuffer.wrap(testData), false);

    consumer.releaseResources();

    assertEquals(0, consumer.buildResult().length);
  }
}
