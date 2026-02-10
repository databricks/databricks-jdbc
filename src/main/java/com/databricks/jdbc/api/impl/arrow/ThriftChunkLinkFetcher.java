package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;

/**
 * ChunkLinkFetcher implementation for the Thrift client.
 *
 * <p>Thrift provides chunk links via the getResultChunks API, which returns links with
 * nextChunkIndex to indicate continuation. When nextChunkIndex is null, it indicates no more
 * chunks.
 */
public class ThriftChunkLinkFetcher implements ChunkLinkFetcher {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(ThriftChunkLinkFetcher.class);

  private final IDatabricksSession session;
  private final StatementId statementId;

  public ThriftChunkLinkFetcher(IDatabricksSession session, StatementId statementId) {
    this.session = session;
    this.statementId = statementId;
    LOGGER.debug("Created ThriftChunkLinkFetcher for statement {}", statementId);
  }

  @Override
  public ChunkLinkFetchResult fetchLinks(long startChunkIndex, long startRowOffset)
      throws SQLException {
    // Thrift uses startRowOffset with FETCH_ABSOLUTE; startChunkIndex is used for metadata
    LOGGER.debug(
        "Fetching links starting from chunk index {}, row offset {} for statement {}",
        startChunkIndex,
        startRowOffset,
        statementId);

    return session
        .getDatabricksClient()
        .getResultChunks(statementId, startChunkIndex, startRowOffset);
  }

  @Override
  public ExternalLink refetchLink(long chunkIndex, long rowOffset) throws SQLException {
    // Thrift uses rowOffset with FETCH_ABSOLUTE
    LOGGER.info(
        "Refetching expired link for chunk {}, row offset {} of statement {}",
        chunkIndex,
        rowOffset,
        statementId);

    // For Thrift, we may need to retry if hasMore=true but no links returned yet
    int maxRetries = 100; // Reasonable limit to prevent infinite loops
    int retryCount = 0;

    while (retryCount < maxRetries) {
      ChunkLinkFetchResult result =
          session.getDatabricksClient().getResultChunks(statementId, chunkIndex, rowOffset);

      if (!result.getChunkLinks().isEmpty()) {
        // Find the link for the requested chunk index
        for (ExternalLink link : result.getChunkLinks()) {
          if (link.getChunkIndex() == chunkIndex) {
            LOGGER.debug(
                "Successfully refetched link for chunk {} of statement {}",
                chunkIndex,
                statementId);
            return link;
          }
        }

        // Exact match not found - this indicates a server bug
        throw new DatabricksSQLException(
            String.format(
                "Failed to refetch link for chunk %d: server returned links but none matched requested index",
                chunkIndex),
            DatabricksDriverErrorCode.CHUNK_READY_ERROR);
      }

      // No links returned - check if we should retry
      if (!result.hasMore()) {
        // No more data and no links - this is unexpected for a refetch
        throw new DatabricksSQLException(
            String.format(
                "Failed to refetch link for chunk %d: no links returned and hasMore=false",
                chunkIndex),
            DatabricksDriverErrorCode.CHUNK_READY_ERROR);
      }

      // hasMore=true but no links yet - retry
      retryCount++;
      LOGGER.debug(
          "No links returned for chunk {} but hasMore=true, retrying ({}/{})",
          chunkIndex,
          retryCount,
          maxRetries);
    }

    throw new DatabricksSQLException(
        String.format(
            "Failed to refetch link for chunk %d: max retries (%d) exceeded",
            chunkIndex, maxRetries),
        DatabricksDriverErrorCode.CHUNK_READY_ERROR);
  }

  @Override
  public void close() {
    LOGGER.debug("Closing ThriftChunkLinkFetcher for statement {}", statementId);
    // No resources to clean up for Thrift fetcher
  }
}
