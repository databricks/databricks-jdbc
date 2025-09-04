package com.databricks.jdbc.api.impl.arrow.incubator;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hc.client5.http.async.methods.AbstractBinResponseConsumer;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpResponse;

/**
 * {@link AbstractBinResponseConsumer} that handles streaming Arrow data chunks. This class
 * processes incoming data in chunks, accumulates them in a byte array output stream, and provides
 * performance metrics for the download operation.
 *
 * <p>The consumer works in conjunction with ArrowResultChunkV2 to handle the asynchronous
 * downloading of Arrow format data.
 */
class StreamingResponseConsumer extends AbstractBinResponseConsumer<byte[]> {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(StreamingResponseConsumer.class);

  private final ArrowResultChunkV2 chunk;
  private long bytesReceived = 0;
  private final ByteArrayOutputStream byteOutputStream;

  public StreamingResponseConsumer(ArrowResultChunkV2 chunk) {
    this.chunk = chunk;
    this.byteOutputStream = new ByteArrayOutputStream();
  }

  @Override
  protected void start(HttpResponse response, ContentType contentType) throws HttpException {
    // Verify response status code here if needed
    if (response.getCode() != 200) {
      throw new HttpException("Unexpected response status: " + response.getCode());
    }
    chunk.downloadStartTime = System.nanoTime();
  }

  /**
   * Defines the size of chunks to process during data consumption.
   *
   * @return the increment size in bytes (1MB in this implementation)
   */
  @Override
  protected int capacityIncrement() {
    // Define the size of chunks to process, e.g., 1MB
    return 1024 * 1024;
  }

  /**
   * Processes incoming chunks of data from the HTTP response. This method is called repeatedly as
   * data becomes available in the {@link java.nio.channels.Channel}. It accumulates the received
   * data and tracks download statistics.
   *
   * @param data the ByteBuffer containing the chunk of response data
   * @param endOfStream flag indicating if this is the last chunk of data
   * @throws IOException if an error occurs while processing the data
   */
  @Override
  protected void data(ByteBuffer data, boolean endOfStream) throws IOException {
    try {
      int currentBatch = data.remaining();
      bytesReceived += currentBatch;

      // Write data as it comes in
      byte[] bytes = new byte[currentBatch];
      data.get(bytes);
      byteOutputStream.write(bytes);

      if (endOfStream) {
        chunk.downloadEndTime = System.nanoTime(); // Record end time
        chunk.bytesDownloaded = bytesReceived;
        logDownloadStats();
      }
    } catch (IOException e) {
      failed(e);
      throw e;
    }
  }

  @Override
  protected byte[] buildResult() {
    return byteOutputStream.toByteArray();
  }

  @Override
  public void failed(Exception cause) {
    byteOutputStream.reset();
  }

  @Override
  public void releaseResources() {
    byteOutputStream.reset();
  }

  /**
   * Logs download statistics including size, duration, and speed. This method is called when the
   * download is complete. The statistics are logged at debug level and include:
   *
   * <ul>
   *   <li>Total size in MB
   *   <li>Duration in milliseconds
   *   <li>Download speed in MB/s
   * </ul>
   */
  private void logDownloadStats() {
    double durationMs = (chunk.downloadEndTime - chunk.downloadStartTime) / 1_000_000.0;
    double speedMBps = (chunk.bytesDownloaded / 1024.0 / 1024.0) / (durationMs / 1000.0);

    LOGGER.debug(
        "Download stats for chunk %s: Size: %s MB, Duration: %s ms, Speed: %s MB/s",
        chunk.getChunkIndex(), chunk.bytesDownloaded / 1024.0 / 1024.0, durationMs, speedMBps);
  }
}
