package com.databricks.jdbc.core;

import static com.databricks.jdbc.TestConstants.TEST_TABLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.impl.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.client.impl.thrift.generated.TSparkArrowBatch;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ChunkExtractorTest {
  @Mock TGetResultSetMetadataResp metadata;

  @Test
  void testInitialisation() throws DatabricksParsingException {
    TSparkArrowBatch arrowBatch =
        new TSparkArrowBatch().setRowCount(0).setBatch(new byte[] {65, 66, 67});
    when(metadata.getArrowSchema()).thenReturn(null);
    when(metadata.getSchema()).thenReturn(TEST_TABLE_SCHEMA);
    ChunkExtractor chunkExtractor =
        new ChunkExtractor(Collections.singletonList(arrowBatch), metadata);
    assertTrue(chunkExtractor.hasNext());
    assertNotNull(chunkExtractor.next());
  }
}
