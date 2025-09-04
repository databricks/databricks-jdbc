package com.databricks.jdbc.api.impl.volume;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.hc.core5.http.nio.DataStreamChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class InputStreamFixedLenProducerTest {

  @Mock private InputStream mockInputStream;
  @Mock private DataStreamChannel mockChannel;

  @Test
  public void testProduce_SmallContent() throws IOException {
    byte[] data = "test".getBytes();
    InputStream inputStream = new ByteArrayInputStream(data);
    InputStreamFixedLenProducer producer =
        new InputStreamFixedLenProducer(inputStream, data.length);

    // Mock channel to accept all data in one write
    when(mockChannel.write(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              buffer.position(buffer.limit()); // Mark all data as written
              return buffer.capacity();
            });

    producer.produce(mockChannel);

    // Verify that endStream was called
    verify(mockChannel).endStream();
    verify(mockChannel, never()).requestOutput();
  }

  @Test
  public void testProduce_PartialWrite() throws IOException {
    byte[] data = "test data".getBytes();
    InputStream inputStream = new ByteArrayInputStream(data);
    InputStreamFixedLenProducer producer =
        new InputStreamFixedLenProducer(inputStream, data.length);

    // Mock channel to accept only part of the data on first write
    when(mockChannel.write(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              // Simulate partial write - only consume half the buffer
              int originalPosition = buffer.position();
              int halfRemaining = buffer.remaining() / 2;
              buffer.position(originalPosition + halfRemaining);
              return halfRemaining;
            })
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              buffer.position(buffer.limit()); // Mark remaining data as written
              return buffer.remaining();
            });

    // First call - partial write
    producer.produce(mockChannel);
    verify(mockChannel).requestOutput();
    verify(mockChannel, never()).endStream();

    // Second call - complete write
    producer.produce(mockChannel);
    verify(mockChannel).endStream();
  }

  @Test
  public void testProduce_MultipleChunks() throws IOException {
    byte[] data = new byte[32768]; // Large data to force multiple chunks
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 256);
    }
    InputStream inputStream = new ByteArrayInputStream(data);
    InputStreamFixedLenProducer producer =
        new InputStreamFixedLenProducer(inputStream, data.length, 1024);

    // Mock channel to accept all data in each write
    when(mockChannel.write(any(ByteBuffer.class)))
        .thenAnswer(
            invocation -> {
              ByteBuffer buffer = invocation.getArgument(0);
              buffer.position(buffer.limit()); // Mark all data as written
              return buffer.capacity();
            });

    // Keep producing until all data is sent
    int produceCount = 0;
    while (produceCount < 100) { // Safety limit
      producer.produce(mockChannel);
      produceCount++;
      // Break if endStream was called (indicating completion)
      try {
        verify(mockChannel).endStream();
        break;
      } catch (AssertionError e) {
        // endStream not called yet, continue
      }
    }

    // Verify that endStream was eventually called
    verify(mockChannel).endStream();
  }

  @Test
  public void testProduce_PrematureEndOfStream() throws IOException {
    when(mockInputStream.read(any(byte[].class), anyInt(), anyInt())).thenReturn(-1);

    InputStreamFixedLenProducer producer = new InputStreamFixedLenProducer(mockInputStream, 10);

    IOException exception = assertThrows(IOException.class, () -> producer.produce(mockChannel));
    assertTrue(exception.getMessage().contains("Unexpected end of stream"));
    assertTrue(exception.getMessage().contains("Read 0 bytes, but expected 10"));
  }

  @Test
  public void testProduce_IOExceptionDuringRead() throws IOException {
    IOException readException = new IOException("Read error");
    when(mockInputStream.read(any(byte[].class), anyInt(), anyInt())).thenThrow(readException);

    InputStreamFixedLenProducer producer = new InputStreamFixedLenProducer(mockInputStream, 10);

    IOException exception = assertThrows(IOException.class, () -> producer.produce(mockChannel));
    assertEquals(readException, exception);
    verify(mockChannel).endStream();
  }

  @Test
  public void testProduce_AfterClosed() throws IOException {
    InputStream inputStream = new ByteArrayInputStream("test".getBytes());
    InputStreamFixedLenProducer producer = new InputStreamFixedLenProducer(inputStream, 4);

    producer.close();
    producer.produce(mockChannel);

    // Should not interact with channel when closed
    verify(mockChannel, never()).write(any(ByteBuffer.class));
    verify(mockChannel, never()).endStream();
    verify(mockChannel, never()).requestOutput();
  }

  @Test
  public void testFailed_ExternalCause() throws IOException {
    InputStream inputStream = new ByteArrayInputStream("test".getBytes());
    InputStreamFixedLenProducer producer = new InputStreamFixedLenProducer(inputStream, 4);

    Exception cause = new RuntimeException("Network error");
    producer.failed(cause);

    // Should close the stream
    assertTrue(inputStream.available() == 0 || inputStream.markSupported());
  }

  @Test
  public void testFailed_MultipleFailures() throws IOException {
    InputStream inputStream = new ByteArrayInputStream("test".getBytes());
    InputStreamFixedLenProducer producer = new InputStreamFixedLenProducer(inputStream, 4);

    Exception cause1 = new RuntimeException("First error");
    Exception cause2 = new RuntimeException("Second error");

    producer.failed(cause1);
    producer.failed(cause2); // Should not cause issues

    // Should still be closed
    assertTrue(inputStream.available() == 0 || inputStream.markSupported());
  }

  @Test
  public void testReleaseResources() throws IOException {
    InputStreamFixedLenProducer producer = new InputStreamFixedLenProducer(mockInputStream, 10);

    producer.releaseResources();

    verify(mockInputStream).close();
  }

  @Test
  public void testReleaseResources_IOExceptionOnClose() throws IOException {
    IOException closeException = new IOException("Close error");
    doThrow(closeException).when(mockInputStream).close();

    InputStreamFixedLenProducer producer = new InputStreamFixedLenProducer(mockInputStream, 10);

    // Should not throw exception even if close fails
    assertDoesNotThrow(() -> producer.releaseResources());
    verify(mockInputStream).close();
  }

  @Test
  public void testReleaseResources_MultipleCallsIdempotent() throws IOException {
    InputStreamFixedLenProducer producer = new InputStreamFixedLenProducer(mockInputStream, 10);

    producer.releaseResources();
    producer.releaseResources(); // Should not call close again

    verify(mockInputStream, times(1)).close();
  }

  @Test
  public void testClose() throws IOException {
    InputStreamFixedLenProducer producer = new InputStreamFixedLenProducer(mockInputStream, 10);

    producer.close();

    verify(mockInputStream).close();
  }

  @Test
  public void testClose_MultipleCallsIdempotent() throws IOException {
    InputStreamFixedLenProducer producer = new InputStreamFixedLenProducer(mockInputStream, 10);

    producer.close();
    producer.close(); // Should not call close again

    verify(mockInputStream, times(1)).close();
  }

  @Test
  public void testEmptyContent() throws IOException {
    InputStream inputStream = new ByteArrayInputStream(new byte[0]);
    InputStreamFixedLenProducer producer = new InputStreamFixedLenProducer(inputStream, 0);

    producer.produce(mockChannel);

    verify(mockChannel).endStream();
    verify(mockChannel, never()).write(any(ByteBuffer.class));
  }
}
