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
 * ChunkLinkFetcher implementation for the SQL Execution API (SEA) client.
 *
 * <p>SEA provides chunk links via the getResultChunks API, which returns links with nextChunkIndex
 * to indicate continuation. When nextChunkIndex is null, it indicates no more chunks.
 */
public class SeaChunkLinkFetcher implements ChunkLinkFetcher {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(SeaChunkLinkFetcher.class);

  private final IDatabricksSession session;
  private final StatementId statementId;

  public SeaChunkLinkFetcher(IDatabricksSession session, StatementId statementId) {
    this.session = session;
    this.statementId = statementId;
    LOGGER.debug("Created SeaChunkLinkFetcher for statement {}", statementId);
  }

  @Override
  public ChunkLinkFetchResult fetchLinks(long startChunkIndex, long startRowOffset)
      throws SQLException {
    // SEA uses startChunkIndex; startRowOffset is ignored
    LOGGER.debug(
        "Fetching links starting from chunk index {} for statement {}",
        startChunkIndex,
        statementId);

    return session
        .getDatabricksClient()
        .getResultChunks(statementId, startChunkIndex, startRowOffset);
  }

  @Override
  public ExternalLink refetchLink(long chunkIndex, long rowOffset) throws SQLException {
    // SEA uses chunkIndex; rowOffset is ignored
    LOGGER.info("Refetching expired link for chunk {} of statement {}", chunkIndex, statementId);

    ChunkLinkFetchResult result =
        session.getDatabricksClient().getResultChunks(statementId, chunkIndex, rowOffset);

    if (result.isEndOfStream()) {
      throw new DatabricksSQLException(
          String.format("Failed to refetch link for chunk %d: no links returned", chunkIndex),
          DatabricksDriverErrorCode.CHUNK_READY_ERROR);
    }

    // Find the link for the requested chunk index
    for (ExternalLink link : result.getChunkLinks()) {
      if (link.getChunkIndex() == chunkIndex) {
        LOGGER.debug(
            "Successfully refetched link for chunk {} of statement {}", chunkIndex, statementId);
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

  @Override
  public void close() {
    LOGGER.debug("Closing SeaChunkLinkFetcher for statement {}", statementId);
    // No resources to clean up for SEA fetcher
  }
}
