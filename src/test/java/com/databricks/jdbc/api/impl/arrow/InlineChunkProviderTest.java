package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.TestConstants.TEST_STATEMENT_ID;
import static com.databricks.jdbc.TestConstants.TEST_TABLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.client.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowBatch;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class InlineChunkProviderTest {
  @Mock TGetResultSetMetadataResp metadata;

  @Test
  void testInitialisation() throws DatabricksParsingException {
    TSparkArrowBatch arrowBatch =
        new TSparkArrowBatch().setRowCount(0).setBatch(new byte[] {65, 66, 67});
    when(metadata.getArrowSchema()).thenReturn(null);
    when(metadata.getSchema()).thenReturn(TEST_TABLE_SCHEMA);
    InlineChunkProvider inlineChunkProvider =
        new InlineChunkProvider(Collections.singletonList(arrowBatch), metadata, TEST_STATEMENT_ID);
    assertTrue(inlineChunkProvider.hasNextChunk());
    assertTrue(inlineChunkProvider.next());
    assertFalse(inlineChunkProvider.next());
  }

  @Test
  void handleErrorTest() throws DatabricksParsingException {
    TSparkArrowBatch arrowBatch =
        new TSparkArrowBatch().setRowCount(0).setBatch(new byte[] {65, 66, 67});
    InlineChunkProvider inlineChunkProvider =
        new InlineChunkProvider(Collections.singletonList(arrowBatch), metadata, TEST_STATEMENT_ID);
    assertThrows(
        DatabricksParsingException.class,
        () -> inlineChunkProvider.handleError(new RuntimeException()));
  }
}
