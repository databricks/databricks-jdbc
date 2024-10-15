package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.exception.DatabricksSQLException;

/**
 * Implementations of this interface manage the retrieval and iteration over {@link
 * ArrowResultChunk}s.
 */
public interface ChunkProvider {

  /**
   * Checks if there are more chunks available to iterate over.
   *
   * @return {@code true} if there are additional chunks to be retrieved; {@code false} otherwise.
   */
  boolean hasNextChunk();

  /**
   * Advances to the next available chunk. This method should be called before calling {@link
   * #getChunk()} to retrieve the data from the next chunk.
   *
   * @return {@code true} if the next chunk was successfully moved to; {@code false} if there are no
   *     more chunks.
   */
  boolean next();

  /**
   * Retrieves the current chunk of data after a successful call to {@link #next()}.
   *
   * @return The current {@link ArrowResultChunk} containing the data.
   * @throws DatabricksSQLException if an error occurs while fetching the chunk.
   */
  ArrowResultChunk getChunk() throws DatabricksSQLException;

  /**
   * Closes the chunk provider and releases any resources associated with it. After calling this
   * method, the chunk provider should not be used again.
   */
  void close();
}
