package com.databricks.jdbc.api.impl.streaming;

import static com.databricks.jdbc.common.util.ArrowUtil.createArrowByteStream;
import static com.databricks.jdbc.common.util.ArrowUtil.getSerializedSchema;
import static com.databricks.jdbc.common.util.ArrowUtil.getTotalRowsInResponse;

import com.databricks.jdbc.api.impl.arrow.ArrowResultChunk;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.ByteArrayInputStream;
import java.util.function.Consumer;

/**
 * Processes Thrift responses into Arrow chunks.
 *
 * <p>This processor converts {@link TFetchResultsResp} into {@link ArrowResultChunk} for inline
 * Arrow result handling. It caches the Arrow schema from the first response for use in subsequent
 * batches.
 */
public class InlineArrowResponseProcessor implements ThriftResponseProcessor<ArrowResultChunk> {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(InlineArrowResponseProcessor.class);

  private final StatementId statementId;
  private volatile byte[]
      cachedSchema; // Cache schema from first response, volatile for visibility across threads

  /**
   * Creates a new inline Arrow response processor.
   *
   * @param statementId The statement ID for logging and chunk creation
   */
  public InlineArrowResponseProcessor(StatementId statementId) {
    this.statementId = statementId;
  }

  @Override
  public StreamingBatch<ArrowResultChunk> processInitialResponse(TFetchResultsResp response)
      throws DatabricksSQLException {
    LOGGER.debug("Processing initial inline Arrow response");
    // Cache the schema for subsequent batches
    try {
      this.cachedSchema = getSerializedSchema(response.getResultSetMetadata());
    } catch (DatabricksParsingException e) {
      LOGGER.error("Failed to serialize Arrow schema: {}", e.getMessage(), e);
      throw new DatabricksSQLException(
          "Failed to serialize Arrow schema",
          e,
          DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
    }
    return processResponse(response, 0, 0);
  }

  @Override
  public StreamingBatch<ArrowResultChunk> processResponse(
      TFetchResultsResp response, long batchIndex, long rowOffset) throws DatabricksSQLException {

    StreamingBatch<ArrowResultChunk> batch =
        new StreamingBatch<>(batchIndex, rowOffset, getReleaseAction());

    try {
      ByteArrayInputStream byteStream = createArrowByteStream(cachedSchema, response, getClass());
      long rowCount = getTotalRowsInResponse(response);

      ArrowResultChunk.Builder builder =
          ArrowResultChunk.builder().withInputStream(byteStream, rowCount);

      if (statementId != null) {
        builder.withStatementId(statementId);
      }

      ArrowResultChunk chunk = builder.build();
      batch.setData(chunk, rowCount, response.hasMoreRows);

      LOGGER.debug(
          "Processed inline Arrow batch {}: rows={}, hasMoreRows={}",
          batchIndex,
          rowCount,
          response.hasMoreRows);

      return batch;

    } catch (DatabricksParsingException e) {
      LOGGER.error("Failed to process inline Arrow batch {}: {}", batchIndex, e.getMessage(), e);
      batch.setError(e);
      throw new DatabricksSQLException(
          "Failed to process Arrow data", e, DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
    }
  }

  @Override
  public Consumer<ArrowResultChunk> getReleaseAction() {
    // Arrow chunks require explicit native memory cleanup
    return chunk -> {
      if (chunk != null) {
        chunk.releaseChunk();
        LOGGER.debug("Released Arrow chunk native memory");
      }
    };
  }
}
