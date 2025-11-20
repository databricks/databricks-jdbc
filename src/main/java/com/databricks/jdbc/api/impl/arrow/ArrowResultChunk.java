package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.util.DatabricksThriftUtil.createExternalLink;
import static com.databricks.jdbc.common.util.ValidationUtil.checkHTTPError;
import static com.databricks.jdbc.telemetry.TelemetryHelper.getStatementIdString;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.common.util.DecompressionUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.telemetry.latency.TelemetryCollector;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;

public class ArrowResultChunk extends AbstractArrowResultChunk {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowResultChunk.class);

  private ArrowResultChunk(Builder builder) throws DatabricksParsingException {
    super(
        builder.numRows,
        builder.rowOffset,
        builder.chunkIndex,
        builder.statementId,
        builder.status,
        builder.chunkLink,
        builder.expiryTime,
        builder.chunkReadyTimeoutSeconds);
    if (builder.inputStream != null) {
      // Data is already available
      try {
        initializeData(builder.inputStream);
      } catch (DatabricksSQLException | IOException e) {
        handleFailure(e, ChunkStatus.PROCESSING_FAILED);
      }
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Downloads and processes the Arrow data chunk using the provided HTTP client and compression
   * codec. Makes a synchronous HTTP GET request to fetch the data, decompresses it, and initializes
   * the chunk's data structures.
   *
   * @param httpClient the HTTP client used to download the chunk data
   * @param compressionCodec the codec used to decompress the downloaded data
   * @throws DatabricksParsingException if there is an error parsing or processing the data
   * @throws IOException if there is an error during download or data reading
   */
  @Override
  protected void downloadData(
      IDatabricksHttpClient httpClient, CompressionCodec compressionCodec, double speedThreshold)
      throws DatabricksParsingException, IOException {
    CloseableHttpResponse response = null;
    long startTime = System.nanoTime();
    try {
      URIBuilder uriBuilder = new URIBuilder(chunkLink.getExternalLink());
      HttpGet getRequest = new HttpGet(uriBuilder.build());
      addHeaders(getRequest, chunkLink.getHttpHeaders());
      // Retry would be done in http client, we should not bother about that here
      response = httpClient.execute(getRequest, true);
      checkHTTPError(response);
      long downloadTimeMs = (System.nanoTime() - startTime) / 1_000_000;

      // Add telemetry for the time to first byte for chunk download
      TelemetryCollector.getInstance()
          .recordChunkDownloadLatency(
              getStatementIdString(statementId), chunkIndex, downloadTimeMs);

      // Read compressed stream fully (download latency excludes decompression)
      byte[] compressed = IOUtils.toByteArray(response.getEntity().getContent());

      // Set status to DOWNLOAD_SUCCEEDED after reading the compressed stream fully
      setStatus(ChunkStatus.DOWNLOAD_SUCCEEDED);

      long size = response.getEntity().getContentLength();
      logDownloadMetrics(
          downloadTimeMs,
          size > 0 ? size : compressed.length,
          chunkLink.getExternalLink(),
          speedThreshold);

      // Decompress (if needed) and parse
      try {
        String ctx =
            String.format(
                "Data decompression for chunk index [%d] and statement [%s]",
                this.chunkIndex, this.statementId);
        InputStream data = DecompressionUtil.decompressToStream(compressed, compressionCodec, ctx);
        initializeData(data);
      } catch (Exception e) {
        handleFailure(e, ChunkStatus.PROCESSING_FAILED);
      }
    } catch (Exception e) {
      handleFailure(e, ChunkStatus.DOWNLOAD_FAILED);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Handles failures that occur during chunk download or processing. Sets the error message,
   * logs the error, updates the chunk status, and throws a DatabricksParsingException.
   *
   * @param exception the exception that caused the failure
   * @param failedStatus the status to set for the chunk after failure (e.g. {@link
   *     ChunkStatus#DOWNLOAD_FAILED} or {@link ChunkStatus#PROCESSING_FAILED})
   * @throws DatabricksParsingException always thrown with the error message and original exception
   */
  @Override
  protected void handleFailure(Exception exception, ChunkStatus failedStatus)
      throws DatabricksParsingException {
    errorMessage =
        String.format(
            "Data parsing failed for chunk index [%d] and statement [%s]. Exception [%s]",
            this.chunkIndex, this.statementId, exception);
    LOGGER.error(this.errorMessage);
    setStatus(failedStatus);
    throw new DatabricksParsingException(errorMessage, exception, failedStatus.toString());
  }

  private void addHeaders(HttpGet getRequest, Map<String, String> headers) {
    if (headers != null) {
      headers.forEach(getRequest::addHeader);
    } else {
      LOGGER.debug(
          "No encryption headers present for chunk index %s and statement %s",
          chunkIndex, statementId);
    }
  }

  private void logDownloadMetrics(
      long downloadTimeMs, long contentLength, String url, double speedThreshold) {
    if (downloadTimeMs > 0 && contentLength > 0) {
      double speedMBps = (contentLength / 1024.0 / 1024.0) / (downloadTimeMs / 1000.0);
      String baseUrl = url.split("\\?")[0];

      LOGGER.info(
          String.format(
              "CloudFetch download: %.4f MB/s, %d bytes in %dms from %s",
              speedMBps, contentLength, downloadTimeMs, baseUrl));

      if (speedMBps < speedThreshold) {
        LOGGER.warn(
            String.format(
                "CloudFetch download slower than threshold: %.4f MB/s < %.4f MB/s",
                speedMBps, speedThreshold));
      }
    }
  }

  public static class Builder {
    private long chunkIndex;
    private long numRows;
    private long rowOffset;
    private ExternalLink chunkLink;
    private StatementId statementId;
    private Instant expiryTime;
    private ChunkStatus status;
    private InputStream inputStream;
    private int chunkReadyTimeoutSeconds =
        Integer.parseInt(DatabricksJdbcUrlParams.CHUNK_READY_TIMEOUT_SECONDS.getDefaultValue());

    public Builder withStatementId(StatementId statementId) {
      this.statementId = statementId;
      return this;
    }

    public Builder withChunkInfo(BaseChunkInfo baseChunkInfo) {
      this.chunkIndex = baseChunkInfo.getChunkIndex();
      this.numRows = baseChunkInfo.getRowCount();
      this.rowOffset = baseChunkInfo.getRowOffset();
      this.status = status == null ? ChunkStatus.PENDING : status;
      return this;
    }

    public Builder withInputStream(InputStream stream, long rowCount) {
      this.numRows = rowCount;
      this.inputStream = stream;
      this.status = status == null ? ChunkStatus.DOWNLOAD_SUCCEEDED : status;
      return this;
    }

    public Builder withThriftChunkInfo(long chunkIndex, TSparkArrowResultLink chunkInfo) {
      this.chunkIndex = chunkIndex;
      this.numRows = chunkInfo.getRowCount();
      this.rowOffset = chunkInfo.getStartRowOffset();
      this.expiryTime = Instant.ofEpochMilli(chunkInfo.getExpiryTime());
      this.status =
          status == null
              ? ChunkStatus.URL_FETCHED
              : status; // URL has always been fetched in case of thrift
      this.chunkLink = createExternalLink(chunkInfo, chunkIndex);
      return this;
    }

    public Builder withChunkStatus(ChunkStatus status) {
      this.status = status;
      return this;
    }

    public Builder withChunkReadyTimeoutSeconds(int chunkReadyTimeoutSeconds) {
      this.chunkReadyTimeoutSeconds = chunkReadyTimeoutSeconds;
      return this;
    }

    public ArrowResultChunk build() throws DatabricksParsingException {
      return new ArrowResultChunk(this);
    }
  }
}
