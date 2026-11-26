package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.impl.thrift.StreamingColumnarResult;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import java.sql.SQLException;

/** SEA columnar result backed by the shared Thrift columnar streaming implementation. */
public class ColumnarDataResult implements IExecutionResult {
  private final StreamingColumnarResult delegate;

  public ColumnarDataResult(
      ResultManifest manifest, ResultData data, StatementId statementId, IDatabricksSession session)
      throws DatabricksSQLException {
    SeaColumnarBatchFetcher fetcher =
        new SeaColumnarBatchFetcher(data, manifest, statementId, session);
    delegate = new StreamingColumnarResult(fetcher.getInitialResponse(), fetcher, session);
  }

  @Override
  public Object getObject(int columnIndex) throws DatabricksSQLException {
    return delegate.getObject(columnIndex);
  }

  @Override
  public long getCurrentRow() {
    return delegate.getCurrentRow();
  }

  @Override
  public boolean next() throws SQLException {
    return delegate.next();
  }

  @Override
  public boolean hasNext() {
    return delegate.hasNext();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public long getRowCount() {
    return delegate.getRowCount();
  }

  @Override
  public long getChunkCount() {
    return delegate.getChunkCount();
  }

  public long getTotalRowsFetched() {
    return delegate.getTotalRowsFetched();
  }
}
