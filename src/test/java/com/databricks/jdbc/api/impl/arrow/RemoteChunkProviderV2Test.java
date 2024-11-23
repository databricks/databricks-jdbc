package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.service.sql.ResultSchema;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RemoteChunkProviderV2Test {

  private static final StatementId STATEMENT_ID = new StatementId("statement_id");

  @Test
  public void testInitEmptyChunkDownloader() {
    ResultManifest resultManifest =
        new ResultManifest()
            .setTotalChunkCount(0L)
            .setTotalRowCount(0L)
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()));
    ResultData resultData = new ResultData().setExternalLinks(new ArrayList<>());
    assertDoesNotThrow(
        () -> new RemoteChunkProviderV2(STATEMENT_ID, resultManifest, resultData, null, null, 4));
  }
}
