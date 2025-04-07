package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.service.sql.ResultSchema;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RemoteChunkProviderTest {

  private static final StatementId STATEMENT_ID = new StatementId("statement_id");
  @Mock private IDatabricksSession mockSession;

  @Test
  public void testInitEmptyChunkDownloader() {
    ResultManifest resultManifest =
        new ResultManifest()
            .setTotalChunkCount(0L)
            .setTotalRowCount(0L)
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()));
    ResultData resultData = new ResultData().setExternalLinks(new ArrayList<>());
    when(mockSession.getConnectionContext()).thenReturn(mock(IDatabricksConnectionContext.class));
    when(mockSession.getConnectionContext().getClientType()).thenReturn(DatabricksClientType.SEA);
    assertDoesNotThrow(
        () ->
            new RemoteChunkProvider(
                STATEMENT_ID, resultManifest, resultData, mockSession, null, 4));
  }
}
