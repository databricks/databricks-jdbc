package com.databricks.jdbc.api.impl.streaming;

import static com.databricks.jdbc.common.util.DatabricksThriftUtil.createColumnarView;

import com.databricks.jdbc.api.impl.ColumnarRowView;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import java.util.function.Consumer;

/**
 * Processes Thrift responses into columnar view data.
 *
 * <p>This processor converts {@link TFetchResultsResp} into {@link ColumnarRowView} for Thrift
 * columnar result handling.
 */
public class ColumnarResponseProcessor implements ThriftResponseProcessor<ColumnarRowView> {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(ColumnarResponseProcessor.class);

  @Override
  public StreamingBatch<ColumnarRowView> processInitialResponse(TFetchResultsResp response)
      throws DatabricksSQLException {
    LOGGER.debug("Processing initial columnar response");
    return processResponse(response, 0, 0);
  }

  @Override
  public StreamingBatch<ColumnarRowView> processResponse(
      TFetchResultsResp response, long batchIndex, long rowOffset) throws DatabricksSQLException {

    StreamingBatch<ColumnarRowView> batch =
        new StreamingBatch<>(batchIndex, rowOffset, getReleaseAction());

    ColumnarRowView view = createColumnarView(response.getResults());
    batch.setData(view, view.getRowCount(), response.hasMoreRows);

    LOGGER.debug(
        "Processed columnar batch {}: rows={}, hasMoreRows={}",
        batchIndex,
        view.getRowCount(),
        response.hasMoreRows);

    return batch;
  }

  @Override
  public Consumer<ColumnarRowView> getReleaseAction() {
    // ColumnarRowView doesn't need explicit cleanup - just let GC handle it
    return view -> {
      /* no-op */
    };
  }
}
