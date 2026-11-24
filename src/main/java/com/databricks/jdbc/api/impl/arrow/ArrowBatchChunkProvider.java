package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;

/** Reads inline ARROW_BATCH pages with bounded background prefetch. */
final class ArrowBatchChunkProvider extends PagedArrowChunkProvider {
  private static final String WORKER_NAME = "apc-arrow-batch-prefetcher";

  ArrowBatchChunkProvider(
      ResultData firstPage,
      ResultManifest resultManifest,
      StatementId statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
    super(firstPage, statementId, session, decoder(resultManifest), WORKER_NAME);
  }

  ArrowBatchChunkProvider(
      ResultData firstPage,
      ResultManifest resultManifest,
      StatementId statementId,
      IDatabricksSession session,
      int maxChunksInMemory,
      int chunkReadyTimeoutSeconds)
      throws DatabricksSQLException {
    super(
        firstPage,
        statementId,
        session,
        decoder(resultManifest),
        maxChunksInMemory,
        chunkReadyTimeoutSeconds,
        WORKER_NAME);
  }

  private static PageDecoder decoder(ResultManifest resultManifest) {
    return new PageDecoder() {
      private long decodedRows;

      @Override
      public ArrowResultChunk decode(ResultData page) throws DatabricksSQLException {
        ArrowResultChunk chunk = ArrowBatchChunkProviderFactory.decodePage(page, resultManifest);
        try {
          decodedRows = Math.addExact(decodedRows, chunk.getNumRows());
        } catch (ArithmeticException e) {
          chunk.releaseChunk();
          throw ArrowBatchChunkProviderFactory.parsingError("Arrow batch row count overflowed");
        }
        if (page.getNextChunkIndex() == null
            && resultManifest.getTotalRowCount() != null
            && resultManifest.getTotalRowCount() != decodedRows) {
          chunk.releaseChunk();
          throw ArrowBatchChunkProviderFactory.parsingError(
              String.format(
                  "Arrow batch row_count total %d does not match manifest total_row_count %d",
                  decodedRows, resultManifest.getTotalRowCount()));
        }
        return chunk;
      }
    };
  }
}
