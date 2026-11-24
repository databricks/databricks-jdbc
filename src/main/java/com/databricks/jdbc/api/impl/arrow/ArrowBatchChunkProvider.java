package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;

/** Reads inline ARROW_BATCH pages on demand. */
final class ArrowBatchChunkProvider extends PagedArrowChunkProvider {
  private static final String WORKER_NAME = "apc-arrow-batch-page-fetcher";

  ArrowBatchChunkProvider(
      ResultData firstPage,
      ResultManifest resultManifest,
      StatementId statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
    this(firstPage, resultManifest, statementId, session, 1, 0);
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
        page -> ArrowBatchChunkProviderFactory.decodePage(page, resultManifest),
        maxChunksInMemory,
        chunkReadyTimeoutSeconds,
        resultManifest.getTotalRowCount(),
        true,
        true,
        false,
        WORKER_NAME);
  }
}
