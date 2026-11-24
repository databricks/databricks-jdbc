package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksHttpTTransport;
import com.databricks.jdbc.model.client.thrift.generated.TCLIService;
import com.databricks.jdbc.model.client.thrift.generated.TCancelOperationReq;
import com.databricks.jdbc.model.client.thrift.generated.TCancelOperationResp;
import com.databricks.jdbc.model.client.thrift.generated.THandleIdentifier;
import com.databricks.jdbc.model.client.thrift.generated.TOperationHandle;
import com.databricks.jdbc.model.client.thrift.generated.TOperationType;
import com.databricks.jdbc.model.client.thrift.generated.TStatus;
import com.databricks.jdbc.model.client.thrift.generated.TStatusCode;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.commons.CommonsHttpClient;
import com.databricks.sdk.core.http.Request;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLTimeoutException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TimeoutExitTest {
  private static final String THRIFT_PATH = "/cliservice";
  private static final String SEA_CANCEL_PATH =
      "/api/2.0/sql/statements/00000000-0000-0000-0000-000000000001/cancel";

  @ParameterizedTest(name = "cancel reaches server before exit, SEA={0}")
  @ValueSource(booleans = {false, true})
  void cancellationReachesServerBeforeImmediateExit(boolean sea) throws Exception {
    AtomicInteger cancellations = new AtomicInteger();
    AtomicReference<Throwable> serverFailure = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try {
            assertEquals("POST", exchange.getRequestMethod());
            assertEquals(sea ? SEA_CANCEL_PATH : THRIFT_PATH, exchange.getRequestURI().getPath());
            byte[] response = sea ? "{}".getBytes(StandardCharsets.UTF_8) : thriftSuccess(exchange);
            cancellations.incrementAndGet();
            exchange
                .getResponseHeaders()
                .set("Content-Type", sea ? "application/json" : "application/x-thrift");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
          } catch (Throwable failure) {
            serverFailure.compareAndSet(null, failure);
            exchange.sendResponseHeaders(500, -1);
          } finally {
            exchange.close();
          }
        });
    server.start();
    Process child = null;
    Path output = Files.createTempFile("jdbc-timeout-exit", ".log");
    try {
      String classpath =
          System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
      child =
          new ProcessBuilder(
                  new File(System.getProperty("java.home"), "bin/java").getPath(),
                  "-cp",
                  classpath,
                  ExitAfterTimeout.class.getName(),
                  Boolean.toString(sea),
                  Integer.toString(server.getAddress().getPort()))
              .redirectErrorStream(true)
              .redirectOutput(output.toFile())
              .start();
      boolean exited = child.waitFor(15, TimeUnit.SECONDS);
      String diagnostics = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
      assertTrue(exited, "Timeout child did not exit within 15 seconds: " + diagnostics);
      assertEquals(0, child.exitValue(), "Timeout child failed: " + diagnostics);
      assertNull(serverFailure.get(), () -> "Invalid cancel RPC: " + serverFailure.get());
      assertEquals(
          1, cancellations.get(), "Cancel RPC must arrive before the timeout caller exits");
    } finally {
      if (child != null && child.isAlive()) {
        child.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
      }
      server.stop(0);
      Files.deleteIfExists(output);
    }
  }

  private static byte[] thriftSuccess(HttpExchange exchange) throws Exception {
    TBinaryProtocol input = new TBinaryProtocol(new TIOStreamTransport(exchange.getRequestBody()));
    TMessage message = input.readMessageBegin();
    assertEquals("CancelOperation", message.name);
    assertEquals(TMessageType.CALL, message.type);
    TCLIService.CancelOperation_args arguments = new TCLIService.CancelOperation_args();
    arguments.read(input);
    input.readMessageEnd();
    assertEquals(cancelRequest(), arguments.getReq());

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    TBinaryProtocol output = new TBinaryProtocol(new TIOStreamTransport(bytes));
    output.writeMessageBegin(new TMessage(message.name, TMessageType.REPLY, message.seqid));
    new TCLIService.CancelOperation_result(
            new TCancelOperationResp(new TStatus(TStatusCode.SUCCESS_STATUS)))
        .write(output);
    output.writeMessageEnd();
    return bytes.toByteArray();
  }

  private static TCancelOperationReq cancelRequest() {
    return new TCancelOperationReq(
        new TOperationHandle(
            new THandleIdentifier(ByteBuffer.wrap(new byte[16]), ByteBuffer.wrap(new byte[16])),
            TOperationType.EXECUTE_STATEMENT,
            false));
  }

  public static class ExitAfterTimeout {
    public static void main(String[] args) throws Exception {
      boolean sea = Boolean.parseBoolean(args[0]);
      String authority = "127.0.0.1:" + Integer.parseInt(args[1]);
      IDatabricksConnectionContext context =
          DatabricksConnectionContext.parse(
              "jdbc:databricks://" + authority + "/default;AuthMech=3;httpPath=" + THRIFT_PATH,
              new Properties());
      Callable<?> cancelRpc = cancelRpc(sea, "http://" + authority, context);
      TimeoutHandler handler =
          new TimeoutHandler(
              1,
              "exit regression",
              () -> {
                assertFalse(Thread.currentThread().isInterrupted(), "Cancel entered interrupted");
                try {
                  Thread.sleep(100);
                  cancelRpc.call();
                } catch (Exception failure) {
                  throw new RuntimeException("Cancel RPC failed", failure);
                }
              },
              DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT,
              context);
      try {
        handler.sleep(10000);
      } catch (SQLTimeoutException expected) {
        System.exit(0);
      }
      throw new AssertionError("Expected SQLTimeoutException");
    }

    private static Callable<?> cancelRpc(
        boolean sea, String url, IDatabricksConnectionContext context) throws Exception {
      if (sea) {
        CommonsHttpClient client = new CommonsHttpClient.Builder().withTimeoutSeconds(5).build();
        return () -> {
          assertEquals(
              200,
              client
                  .execute(
                      new Request(Request.POST, url + SEA_CANCEL_PATH, "{}")
                          .withHeader("Content-Type", "application/json"))
                  .getStatusCode());
          return null;
        };
      }

      PoolingHttpClientConnectionManager pool = new PoolingHttpClientConnectionManager();
      CloseableHttpClient apache = HttpClients.custom().setConnectionManager(pool).build();
      Constructor<DatabricksHttpClient> constructor =
          DatabricksHttpClient.class.getDeclaredConstructor(
              CloseableHttpClient.class, PoolingHttpClientConnectionManager.class);
      constructor.setAccessible(true);
      DatabricksConfig config =
          new DatabricksConfig() {
            @Override
            public synchronized Map<String, String> authenticate() {
              return Collections.emptyMap();
            }
          };
      DatabricksHttpTTransport transport =
          new DatabricksHttpTTransport(
              constructor.newInstance(apache, pool), url + THRIFT_PATH, config, context);
      TCLIService.Client client = new TCLIService.Client(new TBinaryProtocol(transport));
      return () -> client.CancelOperation(cancelRequest());
    }
  }
}
