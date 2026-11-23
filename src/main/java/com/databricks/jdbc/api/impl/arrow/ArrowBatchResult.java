package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import java.util.ArrayList;

/** Keeps SEA native record-batch framing separate from Arrow IPC stream results. */
public final class ArrowBatchResult extends AbstractArrowResult {

  public ArrowBatchResult(
      ResultManifest resultManifest,
      ResultData resultData,
      StatementId statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
    super(
        ArrowBatchChunkProviderFactory.create(resultData, resultManifest, statementId, session),
        resultManifest.getSchema().getColumnCount() == 0
            ? new ArrayList<>()
            : new ArrayList<>(resultManifest.getSchema().getColumns()),
        session);
  }
}
