package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.model.core.ExternalLink;
import java.sql.SQLException;

/**
 * Callback interface for refreshing expired chunk links. Used by {@link StreamingChunkDownloadTask}
 * to delegate link refresh to {@link StreamingChunkProvider}, which coalesces concurrent refresh
 * requests into a single batch RPC.
 */
@FunctionalInterface
interface LinkRefresher {
  /**
   * Refreshes an expired link for the given chunk.
   *
   * @param chunkIndex The chunk index whose link has expired
   * @param rowOffset The row offset of the chunk (used by Thrift for FETCH_ABSOLUTE)
   * @return The refreshed ExternalLink with a new expiration time
   * @throws SQLException if the refresh operation fails
   */
  ExternalLink refreshLink(long chunkIndex, long rowOffset) throws SQLException;
}
