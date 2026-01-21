package com.databricks.jdbc.api.impl.streaming;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.ColumnarRowView;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for ColumnarResponseProcessor. */
public class ColumnarResponseProcessorTest {

  private ColumnarResponseProcessor processor;

  @BeforeEach
  void setUp() {
    processor = new ColumnarResponseProcessor();
  }

  @Test
  void testProcessInitialResponse() throws DatabricksSQLException {
    TFetchResultsResp response = createResponseWithData(2, true);

    StreamingBatch<ColumnarRowView> batch = processor.processInitialResponse(response);

    assertNotNull(batch);
    assertEquals(0, batch.getBatchIndex());
    assertEquals(0, batch.getRowOffset());
    assertEquals(2, batch.getRowCount());
    assertTrue(batch.hasMoreRows());
    assertTrue(batch.isReady());
    assertNotNull(batch.getData());
  }

  @Test
  void testProcessResponse() throws DatabricksSQLException {
    TFetchResultsResp response = createResponseWithData(5, false);

    StreamingBatch<ColumnarRowView> batch = processor.processResponse(response, 3, 100);

    assertEquals(3, batch.getBatchIndex());
    assertEquals(100, batch.getRowOffset());
    assertEquals(5, batch.getRowCount());
    assertFalse(batch.hasMoreRows());
  }

  @Test
  void testProcessEmptyResponse() throws DatabricksSQLException {
    TFetchResultsResp response = createEmptyResponse(true);

    StreamingBatch<ColumnarRowView> batch = processor.processInitialResponse(response);

    assertNotNull(batch);
    assertEquals(0, batch.getRowCount());
    assertTrue(batch.hasMoreRows());
    assertTrue(batch.isReady());
  }

  @Test
  void testDataAccessible() throws DatabricksSQLException {
    TFetchResultsResp response = createResponseWithData(2, false);

    StreamingBatch<ColumnarRowView> batch = processor.processInitialResponse(response);
    ColumnarRowView view = batch.getData();

    assertNotNull(view);
    assertEquals(2, view.getRowCount());
    assertEquals(1, view.getColumnCount());
    assertEquals("value_0", view.getValue(0, 0));
    assertEquals("value_1", view.getValue(1, 0));
  }

  @Test
  void testReleaseActionIsNoOp() throws DatabricksSQLException {
    TFetchResultsResp response = createResponseWithData(1, false);
    StreamingBatch<ColumnarRowView> batch = processor.processInitialResponse(response);

    ColumnarRowView viewBeforeRelease = batch.getData();
    assertNotNull(viewBeforeRelease);

    // Release should work without throwing (no-op for columnar)
    batch.release();

    assertEquals(StreamingBatch.Status.RELEASED, batch.getStatus());
    assertNull(batch.getData()); // Data reference is cleared
  }

  @Test
  void testGetReleaseAction() {
    // Should return a no-op consumer (doesn't throw)
    var releaseAction = processor.getReleaseAction();
    assertNotNull(releaseAction);

    // Should not throw when called with any value
    assertDoesNotThrow(() -> releaseAction.accept(null));

    TFetchResultsResp response = createResponseWithData(1, false);
    try {
      ColumnarRowView view = processor.processInitialResponse(response).getData();
      assertDoesNotThrow(() -> releaseAction.accept(view));
    } catch (DatabricksSQLException e) {
      fail("Should not throw: " + e.getMessage());
    }
  }

  // ==================== Helper Methods ====================

  private TFetchResultsResp createEmptyResponse(boolean hasMoreRows) {
    TFetchResultsResp response = new TFetchResultsResp();
    response.hasMoreRows = hasMoreRows;
    TRowSet rowSet = new TRowSet();
    rowSet.setColumns(Collections.emptyList());
    response.setResults(rowSet);
    return response;
  }

  private TFetchResultsResp createResponseWithData(int rowCount, boolean hasMoreRows) {
    TFetchResultsResp response = new TFetchResultsResp();
    response.hasMoreRows = hasMoreRows;

    TRowSet rowSet = new TRowSet();
    List<TColumn> columns = new ArrayList<>();

    TColumn column = new TColumn();
    TStringColumn stringCol = new TStringColumn();
    List<String> values = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      values.add("value_" + i);
    }
    stringCol.setValues(values);
    column.setStringVal(stringCol);
    columns.add(column);

    rowSet.setColumns(columns);
    response.setResults(rowSet);
    return response;
  }
}
