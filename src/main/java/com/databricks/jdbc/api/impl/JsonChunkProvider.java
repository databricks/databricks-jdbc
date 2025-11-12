package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Chunk provider for JSON_ARRAY format results that handles multiple chunks of data.
 *
 * <p>This provider fetches and combines data from multiple JSON chunks when the result is split
 * across multiple chunks. It follows the same pattern as Arrow chunk providers but works with JSON
 * data arrays instead of Arrow streams.
 */
public class JsonChunkProvider {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(JsonChunkProvider.class);

  private final StatementId statementId;
  private final IDatabricksSession session;
  private final ResultManifest resultManifest;
  private final List<List<Object>> allData;
  private boolean isClosed = false;

  /**
   * Creates a new JsonChunkProvider.
   *
   * @param resultManifest the result manifest containing chunk information
   * @param initialResultData the first chunk of result data
   * @param statementId the statement ID for fetching additional chunks
   * @param session the session for making API calls
   * @throws DatabricksSQLException if there's an error processing the initial data or fetching
   *     chunks
   */
  public JsonChunkProvider(
      ResultManifest resultManifest,
      ResultData initialResultData,
      StatementId statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
    this.resultManifest = resultManifest;
    this.statementId = statementId;
    this.session = session;
    this.allData = new ArrayList<>();

    // Process all chunks
    fetchAndCombineAllChunks(initialResultData);
  }

  /**
   * Fetches and combines data from all chunks.
   *
   * @param initialResultData the first chunk of data
   * @throws DatabricksSQLException if there's an error fetching additional chunks
   */
  private void fetchAndCombineAllChunks(ResultData initialResultData)
      throws DatabricksSQLException {
    LOGGER.debug("Starting to fetch and combine JSON chunks for statement: {}", statementId);

    // Add data from the first chunk
    addDataFromChunk(initialResultData);

    // Check if there are more chunks to fetch
    Long totalChunkCount = Objects.requireNonNullElse(resultManifest.getTotalChunkCount(), 0L);
    if (totalChunkCount > 1) {
      LOGGER.debug("Total chunks to fetch: {}", totalChunkCount);

      // Fetch remaining chunks (starting from chunk index 1, since we already have chunk 0)
      for (long chunkIndex = 1; chunkIndex < totalChunkCount; chunkIndex++) {
        if (isClosed) {
          LOGGER.warn("Provider closed while fetching chunk {}", chunkIndex);
          break;
        }

        try {
          LOGGER.debug("Fetching chunk {} of {}", chunkIndex, totalChunkCount);
          ResultData chunkData =
              session.getDatabricksClient().getResultChunksData(statementId, chunkIndex);
          addDataFromChunk(chunkData);
        } catch (DatabricksSQLException e) {
          LOGGER.error("Failed to fetch chunk {} for statement {}", chunkIndex, statementId, e);
          throw new DatabricksSQLException(
              String.format("Failed to fetch chunk %d: %s", chunkIndex, e.getMessage()),
              e,
              DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR);
        }
      }
    }

    LOGGER.debug(
        "Successfully combined {} total rows from {} chunks", allData.size(), totalChunkCount);
  }

  /**
   * Adds data from a single chunk to the combined data list.
   *
   * @param chunkData the chunk data to add
   */
  private void addDataFromChunk(ResultData chunkData) {
    if (chunkData == null || chunkData.getDataArray() == null) {
      LOGGER.debug("Chunk data or data array is null, skipping");
      return;
    }

    Collection<Collection<String>> dataArray = chunkData.getDataArray();
    LOGGER.debug(
        "Adding {} rows from chunk {}",
        dataArray.size(),
        chunkData.getChunkIndex() != null ? chunkData.getChunkIndex() : "unknown");

    for (Collection<String> row : dataArray) {
      if (row != null) {
        allData.add(new ArrayList<>(row));
      } else {
        allData.add(new ArrayList<>());
      }
    }
  }

  /**
   * Gets the combined data from all chunks.
   *
   * @return the list of all rows from all chunks
   */
  public List<List<Object>> getAllData() {
    return allData;
  }

  /**
   * Gets the total number of chunks that were processed.
   *
   * @return the chunk count
   */
  public long getChunkCount() {
    LOGGER.debug("Getting total chunk count");
    return Objects.requireNonNullElse(resultManifest.getTotalChunkCount(), 0L);
  }

  /** Closes the provider and releases resources. */
  public void close() {
    isClosed = true;
    allData.clear();
    LOGGER.debug("JsonChunkProvider closed for statement: {}", statementId);
  }

  /**
   * Checks if the provider is closed.
   *
   * @return true if closed, false otherwise
   */
  public boolean isClosed() {
    return isClosed;
  }
}
