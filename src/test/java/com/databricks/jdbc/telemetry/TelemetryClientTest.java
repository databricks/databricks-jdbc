package com.databricks.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.jdbc.model.telemetry.TelemetryResponse;
import com.databricks.sdk.core.DatabricksConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHeaders;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
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
  @Mock DatabricksConfig databricksConfig;

  private ScheduledExecutorService createMockScheduler() {
    return mock(ScheduledExecutorService.class);
  }

  private ScheduledExecutorService createRealScheduler() {
    return Executors.newSingleThreadScheduledExecutor();
  }

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
          new TelemetryClient(
              context, MoreExecutors.newDirectExecutorService(), createMockScheduler());

      client.exportEvent(
          new TelemetryFrontendLog(TelemetryLogLevel.ERROR).setFrontendLogEventId("event1"));
      Mockito.verifyNoInteractions(mockHttpClient);
      assertEquals(1, client.getCurrentSize());

      client.exportEvent(
          new TelemetryFrontendLog(TelemetryLogLevel.ERROR).setFrontendLogEventId("event2"));
      Thread.sleep(1000);
      assertEquals(0, client.getCurrentSize());

      client.exportEvent(
          new TelemetryFrontendLog(TelemetryLogLevel.ERROR).setFrontendLogEventId("event3"));
      Mockito.verifyNoMoreInteractions(mockHttpClient);
      assertEquals(1, client.getCurrentSize());

      client.close();
      assertEquals(0, client.getCurrentSize());
    }
  }

  @Test
  public void testExportEvent_authenticated() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(mockHttpClient);
      when(mockHttpClient.execute(any())).thenReturn(mockHttpResponse);
      when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(200);

      Map<String, String> headers = Map.of(HttpHeaders.AUTHORIZATION, "token");
      when(databricksConfig.authenticate()).thenReturn(headers);
      TelemetryResponse response = new TelemetryResponse().setNumSuccess(2L).setNumProtoSuccess(2L);
      when(mockHttpResponse.getEntity())
          .thenReturn(new StringEntity(new ObjectMapper().writeValueAsString(response)));

      IDatabricksConnectionContext context =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      TelemetryClient client =
          new TelemetryClient(
              context,
              MoreExecutors.newDirectExecutorService(),
              createMockScheduler(),
              databricksConfig);

      client.exportEvent(
          new TelemetryFrontendLog(TelemetryLogLevel.ERROR).setFrontendLogEventId("event1"));
      Mockito.verifyNoInteractions(mockHttpClient);
      assertEquals(1, client.getCurrentSize());

      client.exportEvent(
          new TelemetryFrontendLog(TelemetryLogLevel.ERROR).setFrontendLogEventId("event2"));
      Thread.sleep(1000);
      assertEquals(0, client.getCurrentSize());
      ArgumentCaptor<HttpUriRequest> requestCaptor = ArgumentCaptor.forClass(HttpUriRequest.class);
      Mockito.verify(mockHttpClient).execute(requestCaptor.capture());
      // Assert: Check if the Authorization header exists
      assertNotNull(requestCaptor.getValue().getFirstHeader("Authorization"));
      assertEquals("token", requestCaptor.getValue().getFirstHeader("Authorization").getValue());

      client.exportEvent(
          new TelemetryFrontendLog(TelemetryLogLevel.ERROR).setFrontendLogEventId("event3"));
      Mockito.verifyNoMoreInteractions(mockHttpClient);
      assertEquals(1, client.getCurrentSize());

      client.close();
      assertEquals(0, client.getCurrentSize());
    }
  }

  @Test
  public void testExportEventDoesNotThrowErrorsInFailures() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(mockHttpClient);
      when(mockHttpClient.execute(any())).thenReturn(mockHttpResponse);
      when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(400);
      IDatabricksConnectionContext context =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      TelemetryClient client =
          new TelemetryClient(
              context, MoreExecutors.newDirectExecutorService(), createMockScheduler());

      client.exportEvent(
          new TelemetryFrontendLog(TelemetryLogLevel.ERROR).setFrontendLogEventId("event1"));
      assertDoesNotThrow(
          () ->
              client.exportEvent(
                  new TelemetryFrontendLog(TelemetryLogLevel.ERROR)
                      .setFrontendLogEventId("event2")));
    }
  }

  @Test
  public void testPeriodicFlushWithAuthenticatedClient() throws Exception {
    ScheduledExecutorService scheduler = createRealScheduler();
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(mockHttpClient);
      when(mockHttpClient.execute(any())).thenReturn(mockHttpResponse);
      when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(200);
      TelemetryResponse response = new TelemetryResponse().setNumSuccess(1L).setNumProtoSuccess(1L);
      when(mockHttpResponse.getEntity())
          .thenReturn(new StringEntity(new ObjectMapper().writeValueAsString(response)));

      Map<String, String> headers = Map.of(HttpHeaders.AUTHORIZATION, "token");
      when(databricksConfig.authenticate()).thenReturn(headers);

      // JDBC URL with 2 seconds flush interval
      String jdbcUrlWith2SecondsFlush =
          "jdbc:databricks://adb-20.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/ghgjhgj;UserAgentEntry=MyApp;EnableTelemetry=1;TelemetryBatchSize=2;TelemetryFlushInterval=2000";

      IDatabricksConnectionContext context =
          DatabricksConnectionContext.parse(jdbcUrlWith2SecondsFlush, new Properties());
      TelemetryClient client =
          new TelemetryClient(
              context, MoreExecutors.newDirectExecutorService(), scheduler, databricksConfig);

      // Add a single event that won't trigger batch flush
      client.exportEvent(
          new TelemetryFrontendLog(TelemetryLogLevel.ERROR).setFrontendLogEventId("event1"));
      assertEquals(1, client.getCurrentSize());

      // Wait for a short time to verify the periodic flush doesn't trigger immediately
      Thread.sleep(100);
      assertEquals(1, client.getCurrentSize());

      // Wait for 2 seconds to trigger the periodic flush
      Thread.sleep(2000);
      assertEquals(0, client.getCurrentSize());

      client.exportEvent(
          new TelemetryFrontendLog(TelemetryLogLevel.ERROR).setFrontendLogEventId("event2"));
      assertEquals(1, client.getCurrentSize());
      // Close the client to trigger final flush
      client.close();
      assertEquals(0, client.getCurrentSize());
    } finally {
      scheduler.shutdown();
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    }
  }

  @Test
  public void testTimerResetOnBatchSizeFlush() throws Exception {
    TelemetryClient client = null;
    ExecutorService executor = null;
    ScheduledExecutorService scheduler = createRealScheduler();
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(mockHttpClient);
      when(mockHttpClient.execute(any())).thenReturn(mockHttpResponse);
      when(mockHttpResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(200);
      TelemetryResponse response = new TelemetryResponse().setNumSuccess(1L).setNumProtoSuccess(1L);
      when(mockHttpResponse.getEntity())
          .thenReturn(new StringEntity(new ObjectMapper().writeValueAsString(response)));

      // Set up a client with 3 second flush interval and batch size of 2
      String jdbcUrl =
          "jdbc:databricks://adb-20.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/ghgjhgj;UserAgentEntry=MyApp;EnableTelemetry=1;TelemetryBatchSize=2;TelemetryFlushInterval=3000";
      IDatabricksConnectionContext context =
          DatabricksConnectionContext.parse(jdbcUrl, new Properties());
      executor = MoreExecutors.newDirectExecutorService();
      client = new TelemetryClient(context, executor, scheduler);

      // Add events to trigger batch size flush
      client.exportEvent(
          new TelemetryFrontendLog(TelemetryLogLevel.ERROR).setFrontendLogEventId("event1"));
      client.exportEvent(
          new TelemetryFrontendLog(TelemetryLogLevel.ERROR)
              .setFrontendLogEventId("event2")); // This should trigger flush due to batch size

      // assert that the flush occurred
      assertEquals(0, client.getCurrentSize());

      // Wait 2 seconds (less than the flush interval)
      Thread.sleep(2000);

      // Add another event
      client.exportEvent(
          new TelemetryFrontendLog(TelemetryLogLevel.ERROR).setFrontendLogEventId("event3"));

      // Verify it's still in the batch (shouldn't have been flushed yet since timer was reset)
      assertEquals(1, client.getCurrentSize());

      // Wait another 5 seconds (still less than full interval from last flush)
      // NOTE: Ideally flush should happen just after 3 seconds but considering the thread switch
      // timings adding a buffer of 6 more seconds
      Thread.sleep(5000);

      // Verify it's  flushed
      assertEquals(0, client.getCurrentSize());

    } finally {
      // Clean up resources
      if (client != null) {
        client.close();
      }
      if (executor != null) {
        executor.shutdown();
        // Wait for any pending tasks to complete
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          executor.shutdownNow();
        }
      }
      scheduler.shutdown();
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
      // Verify mocks were properly used
      verify(mockHttpClient, atLeastOnce()).execute(any());
      verify(mockHttpResponse, atLeastOnce()).getStatusLine();
      verify(mockStatusLine, atLeastOnce()).getStatusCode();
    }
  }
}
