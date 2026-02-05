package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ExternalLink;
import java.sql.SQLException;

/**
 * Abstraction for fetching chunk links from either SEA or Thrift backend. Implementations handle
 * the protocol-specific details of how links are retrieved.
 *
 * <p>This interface enables a unified streaming approach for chunk downloads regardless of the
 * underlying client type (SEA or Thrift).
 */
public interface ChunkLinkFetcher {

  /**
   * Fetches the next batch of chunk links starting from the given position.
   *
   * <p>The implementation may return one or more links in a single call. The returned {@link
   * ChunkLinkFetchResult} indicates whether more chunks are available.
   *
   * <p>SEA implementations use startChunkIndex while Thrift implementations use startRowOffset.
   * Each implementation uses the parameter relevant to its protocol and ignores the other.
   *
   * @param startChunkIndex The chunk index to start fetching from (used by SEA)
   * @param startRowOffset The row offset to start fetching from (used by Thrift with
   *     FETCH_ABSOLUTE)
   * @return ChunkLinkFetchResult containing the fetched links and continuation information
   * @throws SQLException if the fetch operation fails
   */
  ChunkLinkFetchResult fetchLinks(long startChunkIndex, long startRowOffset) throws SQLException;

  /**
   * Refetches a specific chunk link that may have expired.
   *
   * <p>This is used when a previously fetched link has expired before the chunk could be
   * downloaded. Both SEA and Thrift clients support this via the getResultChunks API.
   *
   * <p>SEA uses chunkIndex while Thrift uses rowOffset to identify the chunk to refetch.
   *
   * @param chunkIndex The specific chunk index to refetch (used by SEA)
   * @param rowOffset The row offset of the chunk to refetch (used by Thrift)
   * @return The refreshed ExternalLink with a new expiration time
   * @throws SQLException if the refetch operation fails
   */
  ExternalLink refetchLink(long chunkIndex, long rowOffset) throws SQLException;

  /** Closes any resources held by the fetcher. */
  void close();
}
