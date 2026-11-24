package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.api.impl.arrow.ArrowBatchChunkProviderFactory.parsingError;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** Shared bounded pager for inline Arrow wire formats. */
class PagedArrowChunkProvider implements ChunkProvider {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(PagedArrowChunkProvider.class);

  private final StatementId statementId;
  private final IDatabricksSession session;
  private final PageDecoder decoder;
  private final int maxChunksInMemory;
  private final int chunkReadyTimeoutSeconds;
  private final Long expectedTotalRows;
  private final boolean countLoadedChunks;
  private final boolean skipEmptyPages;
  private final boolean wrapWorkerFailure;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition chunkAvailable = lock.newCondition();
  private final Condition consumerAdvanced = lock.newCondition();
  private final ArrayDeque<ArrowResultChunk> pendingChunks = new ArrayDeque<>();
  private final Thread worker;

  private ArrowResultChunk currentChunk;
  private DatabricksSQLException workerFailure;
  private Long nextChunkIndex;
  private long nextRowOffset;
  private long rowsLoaded;
  private long chunksLoaded;
  private long chunksConsumed;
  private int chunksInMemory;
  private boolean endOfStream;
  private volatile boolean closed;

  PagedArrowChunkProvider(
      ResultData firstPage,
      StatementId statementId,
      IDatabricksSession session,
      PageDecoder decoder,
      int maxChunksInMemory,
      int chunkReadyTimeoutSeconds,
      Long expectedTotalRows,
      boolean countLoadedChunks,
      boolean skipEmptyPages,
      boolean wrapWorkerFailure,
      String workerName)
      throws DatabricksSQLException {
    this.statementId = statementId;
    this.session = session;
    this.decoder = decoder;
    this.maxChunksInMemory = Math.max(1, maxChunksInMemory);
    this.chunkReadyTimeoutSeconds = chunkReadyTimeoutSeconds;
    this.expectedTotalRows = expectedTotalRows;
    this.countLoadedChunks = countLoadedChunks;
    this.skipEmptyPages = skipEmptyPages;
    this.wrapWorkerFailure = wrapWorkerFailure;
    loadPage(firstPage, 0L, 0L);
    if (!endOfStream && (statementId == null || session == null)) {
      worker = null;
      close();
      throw parsingError("Paginated Arrow responses require a session to fetch pages");
    }
    if (endOfStream || this.maxChunksInMemory == 1) {
      worker = null;
    } else {
      worker = new Thread(this::fetchPages, workerName);
      worker.setDaemon(true);
      worker.start();
    }
  }

  @Override
  public boolean hasNextChunk() {
    lock.lock();
    try {
      return !closed && (workerFailure != null || !pendingChunks.isEmpty() || !endOfStream);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean next() throws DatabricksSQLException {
    lock.lock();
    try {
      releaseCurrentChunk();
      while (worker == null && pendingChunks.isEmpty() && workerFailure == null && !endOfStream) {
        long chunkIndex = nextChunkIndex;
        long rowOffset = nextRowOffset;
        lock.unlock();
        try {
          ResultData page =
              session.getDatabricksClient().getResultChunksData(statementId, chunkIndex, rowOffset);
          loadPage(page, chunkIndex, rowOffset);
        } finally {
          lock.lock();
        }
      }
      long remainingNanos =
          chunkReadyTimeoutSeconds <= 0
              ? Long.MAX_VALUE
              : TimeUnit.SECONDS.toNanos(chunkReadyTimeoutSeconds);
      while (!closed && pendingChunks.isEmpty() && workerFailure == null && !endOfStream) {
        try {
          if (chunkReadyTimeoutSeconds <= 0) {
            chunkAvailable.await();
          } else {
            remainingNanos = chunkAvailable.awaitNanos(remainingNanos);
            if (remainingNanos <= 0) {
              throw new DatabricksSQLException(
                  "Timed out waiting for an Arrow result page",
                  DatabricksDriverErrorCode.CHUNK_READY_ERROR);
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new DatabricksSQLException(
              "Interrupted while waiting for an Arrow result page",
              e,
              DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
        }
      }
      if (closed) {
        return false;
      }
      if (!pendingChunks.isEmpty()) {
        currentChunk = pendingChunks.removeFirst();
        chunksConsumed++;
        return true;
      }
      if (workerFailure != null) {
        if (!wrapWorkerFailure) {
          throw workerFailure;
        }
        throw new DatabricksSQLException(
            "Arrow page fetch failed: " + workerFailure.getMessage(),
            workerFailure,
            DatabricksDriverErrorCode.CHUNK_READY_ERROR);
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public ArrowResultChunk getChunk() {
    lock.lock();
    try {
      return currentChunk;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() {
    lock.lock();
    try {
      if (closed) {
        return;
      }
      closed = true;
      chunkAvailable.signalAll();
      consumerAdvanced.signalAll();
    } finally {
      lock.unlock();
    }
    if (worker != null && worker != Thread.currentThread()) {
      worker.interrupt();
      try {
        worker.join(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    lock.lock();
    try {
      releaseCurrentChunk();
      while (!pendingChunks.isEmpty()) {
        releaseChunk(pendingChunks.removeFirst());
      }
      chunksInMemory = 0;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long getRowCount() {
    lock.lock();
    try {
      return rowsLoaded;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long getChunkCount() {
    lock.lock();
    try {
      return countLoadedChunks ? chunksLoaded : chunksConsumed;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  private void fetchPages() {
    while (!closed && !Thread.currentThread().isInterrupted()) {
      long chunkIndex;
      long rowOffset;
      lock.lock();
      try {
        while (!closed && !endOfStream && chunksInMemory >= maxChunksInMemory) {
          consumerAdvanced.await();
        }
        if (closed || endOfStream) {
          return;
        }
        chunkIndex = nextChunkIndex;
        rowOffset = nextRowOffset;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } finally {
        lock.unlock();
      }

      try {
        ResultData page =
            session.getDatabricksClient().getResultChunksData(statementId, chunkIndex, rowOffset);
        loadPage(page, chunkIndex, rowOffset);
      } catch (DatabricksSQLException e) {
        recordFailure(e);
        return;
      } catch (RuntimeException e) {
        recordFailure(
            new DatabricksSQLException(
                "Unexpected Arrow page fetch failure",
                e,
                DatabricksDriverErrorCode.SDK_CLIENT_ERROR));
        return;
      }
    }
  }

  private void loadPage(ResultData page, long expectedChunkIndex, long expectedRowOffset)
      throws DatabricksSQLException {
    validatePage(page, expectedChunkIndex, expectedRowOffset);
    ArrowResultChunk chunk = decoder.decode(page);
    long pageRows = chunk.getNumRows();
    long updatedRows;
    long updatedOffset;
    try {
      updatedRows = Math.addExact(rowsLoaded, pageRows);
      updatedOffset = Math.addExact(expectedRowOffset, pageRows);
    } catch (ArithmeticException e) {
      chunk.releaseChunk();
      throw parsingError("Arrow page row count overflowed");
    }
    Long continuation = page.getNextChunkIndex();
    if (continuation == null
        && expectedTotalRows != null
        && expectedTotalRows.longValue() != updatedRows) {
      chunk.releaseChunk();
      throw parsingError(
          String.format(
              "Arrow page row_count total %d does not match manifest total_row_count %d",
              updatedRows, expectedTotalRows));
    }

    lock.lock();
    try {
      if (closed) {
        chunk.releaseChunk();
        return;
      }
      nextChunkIndex = continuation;
      nextRowOffset = updatedOffset;
      rowsLoaded = updatedRows;
      chunksLoaded++;
      endOfStream = continuation == null;
      if (skipEmptyPages && pageRows == 0) {
        chunk.releaseChunk();
      } else {
        pendingChunks.addLast(chunk);
        chunksInMemory++;
      }
      chunkAvailable.signalAll();
    } finally {
      lock.unlock();
    }
    LOGGER.debug(
        "Loaded Arrow page {} with {} rows for statementId {}, nextChunkIndex={}",
        expectedChunkIndex,
        pageRows,
        statementId == null ? "<initial>" : statementId.toSQLExecStatementId(),
        continuation);
  }

  private static void validatePage(ResultData page, long expectedChunkIndex, long expectedRowOffset)
      throws DatabricksSQLException {
    if (page == null) {
      throw parsingError("Arrow page " + expectedChunkIndex + " is missing result data");
    }
    if (page.getChunkIndex() != null && page.getChunkIndex() != expectedChunkIndex) {
      throw parsingError(
          String.format(
              "Arrow page chunk_index %d does not match requested chunk %d",
              page.getChunkIndex(), expectedChunkIndex));
    }
    if (page.getRowOffset() != null && page.getRowOffset() != expectedRowOffset) {
      throw parsingError(
          String.format(
              "Arrow page row_offset %d does not match requested row offset %d",
              page.getRowOffset(), expectedRowOffset));
    }
    if (page.getNextChunkIndex() != null && page.getNextChunkIndex() <= expectedChunkIndex) {
      throw parsingError(
          String.format(
              "Arrow page %d returned non-increasing next_chunk_index %d",
              expectedChunkIndex, page.getNextChunkIndex()));
    }
  }

  private void recordFailure(DatabricksSQLException failure) {
    lock.lock();
    try {
      if (!closed) {
        workerFailure = failure;
        endOfStream = true;
        chunkAvailable.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }

  private void releaseCurrentChunk() {
    if (currentChunk == null) {
      return;
    }
    ArrowResultChunk chunk = currentChunk;
    currentChunk = null;
    chunksInMemory--;
    consumerAdvanced.signalAll();
    releaseChunk(chunk);
  }

  private static void releaseChunk(ArrowResultChunk chunk) {
    try {
      chunk.releaseChunk();
    } catch (RuntimeException e) {
      LOGGER.warn("Failed to release Arrow result page", e);
    }
  }
}
