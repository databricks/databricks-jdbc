package com.databricks.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.jdbc.model.telemetry.TelemetryResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Properties;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TelemetryClientTest {

  private static final String JDBC_URL =
      "jdbc:databricks://adb-20.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/ghgjhgj;UserAgentEntry=MyApp;EnableTelemetry=1;TelemetryBatchSize=2";

  @Mock IDatabricksHttpClient mockHttpClient;
  @Mock CloseableHttpResponse mockHttpResponse;
  @Mock StatusLine mockStatusLine;

  @Test
  public void testExportEvent() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(mockHttpClient);
      when(mockHttpClient.execute(any())).thenReturn(mockHttpResponse);
      when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(200);
      TelemetryResponse response = new TelemetryResponse().setNumSuccess(2L).setNumProtoSuccess(2L);
      when(mockHttpResponse.getEntity())
          .thenReturn(new StringEntity(new ObjectMapper().writeValueAsString(response)));

      IDatabricksConnectionContext context =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      TelemetryClient client =
          new TelemetryClient(context, false, MoreExecutors.newDirectExecutorService());

      client.exportEvent(new TelemetryFrontendLog().setFrontendLogEventId("event1"));
      Mockito.verifyNoInteractions(mockHttpClient);
      assertEquals(1, client.getCurrentSize());

      client.exportEvent(new TelemetryFrontendLog().setFrontendLogEventId("event2"));
      Thread.sleep(1000);
      assertEquals(0, client.getCurrentSize());

      client.exportEvent(new TelemetryFrontendLog().setFrontendLogEventId("event3"));
      Mockito.verifyNoMoreInteractions(mockHttpClient);
      assertEquals(1, client.getCurrentSize());

      client.close();
      assertEquals(0, client.getCurrentSize());
    }
  }
}
