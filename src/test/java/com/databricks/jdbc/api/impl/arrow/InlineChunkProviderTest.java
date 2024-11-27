package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.TestConstants.ARROW_BATCH_LIST;
import static com.databricks.jdbc.TestConstants.TEST_TABLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.model.client.thrift.generated.TRowSet;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowBatch;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class InlineChunkProviderTest {
  @Mock TGetResultSetMetadataResp metadata;
  @Mock TFetchResultsResp fetchResultsResp;
  @Mock IDatabricksStatementInternal parentStatement;
  @Mock IDatabricksSession session;

  @Test
  void testInitialisation() throws DatabricksParsingException {
    when(fetchResultsResp.getResultSetMetadata()).thenReturn(metadata);
    when(metadata.getArrowSchema()).thenReturn(null);
    when(metadata.getSchema()).thenReturn(TEST_TABLE_SCHEMA);
    when(fetchResultsResp.getResults()).thenReturn(new TRowSet().setArrowBatches(ARROW_BATCH_LIST));
    when(metadata.isSetLz4Compressed()).thenReturn(false);
    InlineChunkProvider inlineChunkProvider =
        new InlineChunkProvider(fetchResultsResp, parentStatement, session);
    assertTrue(inlineChunkProvider.hasNextChunk());
    assertTrue(inlineChunkProvider.next());
    assertFalse(inlineChunkProvider.next());
  }

  @Test
  void handleErrorTest() throws DatabricksParsingException {
    TSparkArrowBatch arrowBatch =
        new TSparkArrowBatch().setRowCount(0).setBatch(new byte[] {65, 66, 67});
    when(fetchResultsResp.getResultSetMetadata()).thenReturn(metadata);
    when(fetchResultsResp.getResults())
        .thenReturn(new TRowSet().setArrowBatches(Collections.singletonList(arrowBatch)));
    InlineChunkProvider inlineChunkProvider =
        new InlineChunkProvider(fetchResultsResp, parentStatement, session);
    assertThrows(
        DatabricksParsingException.class,
        () -> inlineChunkProvider.handleError(new RuntimeException()));
  }
}
