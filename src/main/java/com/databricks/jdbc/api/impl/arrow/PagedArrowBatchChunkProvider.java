package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.api.impl.arrow.ArrowBatchChunkProviderFactory.parsingError;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;

/**
 * Reads an inline ARROW_BATCH result that spans several pages, exposing each page as one chunk.
 *
 * <p>Pages are fetched one at a time by {@code next_chunk_index} and the row offset where the page
 * starts, which the all-purpose-cluster backend turns into a positioned fetch on the cluster. Only
 * the current page is held in memory. Empty pages are skipped so that a reader never sees an empty
 * chunk while more rows remain.
 */
final class PagedArrowBatchChunkProvider implements ChunkProvider {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(PagedArrowBatchChunkProvider.class);

  private final ResultManifest resultManifest;
  private final StatementId statementId;
  private final IDatabricksSession session;
  private ArrowResultChunk currentChunk;
  private boolean currentChunkConsumed;
  private Long nextChunkIndex;
  private long nextRowOffset;
  private long rowsLoaded;
  private boolean isClosed;

  PagedArrowBatchChunkProvider(
      ResultData firstPage,
      ResultManifest resultManifest,
      StatementId statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
    this.resultManifest = resultManifest;
    this.statementId = statementId;
    this.session = session;
    loadPage(firstPage, 0L, 0L);
  }

  @Override
  public boolean hasNextChunk() {
    return !isClosed && (!currentChunkConsumed || nextChunkIndex != null);
  }

  @Override
  public boolean next() throws DatabricksSQLException {
    if (!hasNextChunk()) {
      return false;
    }
    if (!currentChunkConsumed) {
      currentChunkConsumed = true;
      if (currentChunk.getNumRows() > 0 || nextChunkIndex == null) {
        return true;
      }
    }
    do {
      long chunkIndex = nextChunkIndex;
      long rowOffset = nextRowOffset;
      ResultData page =
          session.getDatabricksClient().getResultChunksData(statementId, chunkIndex, rowOffset);
      loadPage(page, chunkIndex, rowOffset);
    } while (currentChunk.getNumRows() == 0 && nextChunkIndex != null);
    return true;
  }

  @Override
  public AbstractArrowResultChunk getChunk() {
    return currentChunk;
  }

  @Override
  public void close() {
    isClosed = true;
    if (currentChunk != null) {
      currentChunk.releaseChunk();
      currentChunk = null;
    }
  }

  /** Rows loaded so far; the total is unknown until the last page has been read. */
  @Override
  public long getRowCount() {
    return rowsLoaded;
  }

  @Override
  public long getChunkCount() {
    return 0;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  private void loadPage(ResultData page, long expectedChunkIndex, long expectedRowOffset)
      throws DatabricksSQLException {
    if (page == null) {
      throw parsingError("ARROW_BATCH page " + expectedChunkIndex + " is missing result data");
    }
    if (page.getChunkIndex() != null && page.getChunkIndex() != expectedChunkIndex) {
      throw parsingError(
          String.format(
              "ARROW_BATCH page chunk_index %d does not match requested chunk %d",
              page.getChunkIndex(), expectedChunkIndex));
    }
    if (page.getRowOffset() != null && page.getRowOffset() != expectedRowOffset) {
      throw parsingError(
          String.format(
              "ARROW_BATCH page row_offset %d does not match requested row offset %d",
              page.getRowOffset(), expectedRowOffset));
    }
    if (page.getNextChunkIndex() != null && page.getNextChunkIndex() <= expectedChunkIndex) {
      throw parsingError(
          String.format(
              "ARROW_BATCH page %d returned non-increasing next_chunk_index %d",
              expectedChunkIndex, page.getNextChunkIndex()));
    }

    ArrowResultChunk chunk = ArrowBatchChunkProviderFactory.decodePage(page, resultManifest);
    if (currentChunk != null) {
      currentChunk.releaseChunk();
    }
    currentChunk = chunk;
    nextChunkIndex = page.getNextChunkIndex();
    nextRowOffset = expectedRowOffset + chunk.getNumRows();
    rowsLoaded += chunk.getNumRows();
    LOGGER.debug(
        "Loaded ARROW_BATCH page {} with {} rows for statementId {}, nextChunkIndex={}",
        expectedChunkIndex,
        chunk.getNumRows(),
        statementId.toSQLExecStatementId(),
        nextChunkIndex);
  }
}
