package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.common.EnvironmentVariables.JDBC_THRIFT_VERSION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksHttpTTransport;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.sdk.core.DatabricksConfig;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

class ThriftOpenSessionRetryTest {

  @ParameterizedTest
  @ValueSource(ints = {503, 429, 502, 504, 408})
  void retriesRealOpenSessionButNotStatementOrClose(int statusCode) throws Exception {
    IDatabricksConnectionContext connectionContext = mock(IDatabricksConnectionContext.class);
    when(connectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
    when(connectionContext.shouldRetryRateLimitError()).thenReturn(true);
    when(connectionContext.getTemporarilyUnavailableRetryTimeout()).thenReturn(900);
    when(connectionContext.getRateLimitRetryTimeout()).thenReturn(120);
    DatabricksConfig config = mock(DatabricksConfig.class);
    when(config.authenticate()).thenReturn(Collections.emptyMap());
    List<Long> sleeps = new ArrayList<>();
    DatabricksHttpRetryHandler retryHandler =
        new DatabricksHttpRetryHandler(connectionContext) {
          @Override
          protected void doSleepForDelay(long delayMillis) {
            sleeps.add(delayMillis);
          }
        };
    AtomicInteger attempts = new AtomicInteger();
    List<byte[]> payloads = Collections.synchronizedList(new ArrayList<>());
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/thrift",
        exchange -> {
          byte[] request = exchange.getRequestBody().readAllBytes();
          payloads.add(request);
          byte[] response = new byte[0];
          int responseStatus = attempts.incrementAndGet() == 1 ? statusCode : 200;
          if (responseStatus == 200) {
            try {
              TMessage message =
                  new TBinaryProtocol(new TMemoryInputTransport(request)).readMessageBegin();
              TMemoryBuffer buffer = new TMemoryBuffer(256);
              TBinaryProtocol protocol = new TBinaryProtocol(buffer);
              protocol.writeMessageBegin(
                  new TMessage(message.name, TMessageType.REPLY, message.seqid));
              new TCLIService.OpenSession_result()
                  .setSuccess(
                      new TOpenSessionResp()
                          .setStatus(new TStatus(TStatusCode.SUCCESS_STATUS))
                          .setServerProtocolVersion(JDBC_THRIFT_VERSION)
                          .setSessionHandle(
                              new TSessionHandle(
                                  new THandleIdentifier(
                                      ByteBuffer.wrap(new byte[16]),
                                      ByteBuffer.wrap(new byte[16])))))
                  .write(protocol);
              protocol.writeMessageEnd();
              response = Arrays.copyOf(buffer.getArray(), buffer.length());
            } catch (TException exception) {
              throw new java.io.IOException(exception);
            }
          }
          exchange.sendResponseHeaders(responseStatus, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    String endpoint = "http://127.0.0.1:" + server.getAddress().getPort() + "/thrift";
    when(connectionContext.getEndpointURL()).thenReturn(endpoint);
    PoolingHttpClientConnectionManager connections = new PoolingHttpClientConnectionManager();
    try (CloseableHttpClient apacheClient =
            HttpClients.custom()
                .setConnectionManager(connections)
                .addInterceptorLast(retryHandler)
                .setRetryHandler(retryHandler)
                .build();
        DatabricksHttpClient httpClient = new DatabricksHttpClient(apacheClient, connections);
        MockedStatic<DatabricksHttpClientFactory> clients =
            mockStatic(DatabricksHttpClientFactory.class);
        MockedStatic<DatabricksClientConfiguratorManager> configurators =
            mockStatic(DatabricksClientConfiguratorManager.class)) {
      DatabricksHttpClientFactory clientFactory = mock(DatabricksHttpClientFactory.class);
      clients.when(DatabricksHttpClientFactory::getInstance).thenReturn(clientFactory);
      when(clientFactory.getClient(connectionContext)).thenReturn(httpClient);
      DatabricksClientConfiguratorManager configuratorManager =
          mock(DatabricksClientConfiguratorManager.class);
      configurators
          .when(DatabricksClientConfiguratorManager::getInstance)
          .thenReturn(configuratorManager);
      ClientConfigurator configurator = mock(ClientConfigurator.class);
      when(configuratorManager.getConfigurator(connectionContext)).thenReturn(configurator);
      when(configurator.getDatabricksConfig()).thenReturn(config);

      DatabricksThriftServiceClient service = new DatabricksThriftServiceClient(connectionContext);
      assertNotNull(
          service.createSession(
              mock(IDatabricksComputeResource.class), null, null, Collections.emptyMap()));
      assertEquals(2, attempts.get());
      assertEquals(List.of(2000L), sleeps);
      assertArrayEquals(payloads.get(0), payloads.get(1));

      TCLIService.Client client =
          new TCLIService.Client(
              new TBinaryProtocol(
                  new DatabricksHttpTTransport(httpClient, endpoint, config, connectionContext)));
      TSessionHandle sessionHandle =
          new TSessionHandle(
              new THandleIdentifier(ByteBuffer.wrap(new byte[16]), ByteBuffer.wrap(new byte[16])));
      attempts.set(0);
      assertThrows(
          TException.class,
          () ->
              client.send_ExecuteStatement(
                  new TExecuteStatementReq()
                      .setSessionHandle(sessionHandle)
                      .setStatement("select 1")));
      assertEquals(1, attempts.get());
      attempts.set(0);
      assertThrows(
          TException.class, () -> client.send_CloseSession(new TCloseSessionReq(sessionHandle)));
      assertEquals(1, attempts.get());
      assertEquals(List.of(2000L), sleeps);
    } finally {
      server.stop(0);
    }
  }
}
