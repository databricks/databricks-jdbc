package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.FAKE_SERVICE_URI_PROP_SUFFIX;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.IS_FAKE_SERVICE_TEST_PROP;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.common.util.UserAgentManager;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksRetryHandlerException;
import com.databricks.sdk.core.ProxyConfig;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksHttpClientTest {
  @Mock CloseableHttpClient mockHttpClient;
  @Mock HttpUriRequest request;
  @Mock PoolingHttpClientConnectionManager connectionManager;
  @Mock CloseableHttpResponse closeableHttpResponse;
  @Mock IDatabricksConnectionContext connectionContext;
  @Mock HttpClientBuilder httpClientBuilder;
  private static final String CLUSTER_JDBC_URL =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;UserAgentEntry=MyApp";
  private static final String DBSQL_JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;UserAgentEntry=MyApp";

  @Test
  public void testSetProxyDetailsIntoHttpClient() {
    HttpClientBuilder builder = HttpClientBuilder.create();

    doReturn(true).when(connectionContext).getUseProxy();
    doReturn("proxyHost").when(connectionContext).getProxyHost();
    doReturn(1234).when(connectionContext).getProxyPort();
    doReturn("proxyUser").when(connectionContext).getProxyUser();
    doReturn("proxyPassword").when(connectionContext).getProxyPassword();
    doReturn(ProxyConfig.ProxyAuthType.BASIC).when(connectionContext).getProxyAuthType();

    assertDoesNotThrow(() -> DatabricksHttpClient.setupProxy(connectionContext, builder));

    doReturn(ProxyConfig.ProxyAuthType.NONE).when(connectionContext).getProxyAuthType();
    assertDoesNotThrow(() -> DatabricksHttpClient.setupProxy(connectionContext, builder));

    doReturn(ProxyConfig.ProxyAuthType.BASIC).when(connectionContext).getProxyAuthType();
    doReturn(null).when(connectionContext).getProxyUser();
    assertThrows(
        IllegalArgumentException.class,
        () -> DatabricksHttpClient.setupProxy(connectionContext, builder));
  }

  @Test
  public void testSetFakeServiceRouteInHttpClient() throws HttpException {
    final String testTargetURI = "https://example.com";
    final String testFakeServiceURI = "http://localhost:8080";
    System.setProperty(testTargetURI + FAKE_SERVICE_URI_PROP_SUFFIX, testFakeServiceURI);

    ArgumentCaptor<HttpRoutePlanner> routePlannerCaptor =
        ArgumentCaptor.forClass(HttpRoutePlanner.class);

    DatabricksHttpClient.setFakeServiceRouteInHttpClient(httpClientBuilder);

    // Capture the route planner set in builder
    Mockito.verify(httpClientBuilder).setRoutePlanner(routePlannerCaptor.capture());
    HttpRoutePlanner capturedRoutePlanner = routePlannerCaptor.getValue();

    // Create a request and determine the route
    HttpGet request = new HttpGet(testTargetURI);
    HttpHost proxy = HttpHost.create(testFakeServiceURI);
    HttpRoute route =
        capturedRoutePlanner.determineRoute(
            HttpHost.create(request.getURI().toString()), request, null);

    // Verify the route is set to the fake service URI
    assertEquals(proxy, route.getProxyHost());
    assertEquals(2, route.getHopCount());

    System.clearProperty(testTargetURI + FAKE_SERVICE_URI_PROP_SUFFIX);
  }

  @Test
  public void testSetFakeServiceRouteInHttpClientWithLocalhostTarget() throws HttpException {
    ArgumentCaptor<HttpRoutePlanner> routePlannerCaptor =
        ArgumentCaptor.forClass(HttpRoutePlanner.class);

    DatabricksHttpClient.setFakeServiceRouteInHttpClient(httpClientBuilder);

    Mockito.verify(httpClientBuilder).setRoutePlanner(routePlannerCaptor.capture());
    HttpRoutePlanner capturedRoutePlanner = routePlannerCaptor.getValue();

    HttpGet request = new HttpGet("http://localhost:53423");
    HttpRoute route =
        capturedRoutePlanner.determineRoute(
            HttpHost.create(request.getURI().toString()), request, null);

    // Verify the route has no proxy host set as the target URI directly points to fake service
    assertNull(route.getProxyHost());
  }

  @Test
  public void testSetFakeServiceRouteInHttpClientThrowsError() {
    final String testTargetURI = "https://example.com";

    ArgumentCaptor<HttpRoutePlanner> routePlannerCaptor =
        ArgumentCaptor.forClass(HttpRoutePlanner.class);

    DatabricksHttpClient.setFakeServiceRouteInHttpClient(httpClientBuilder);

    Mockito.verify(httpClientBuilder).setRoutePlanner(routePlannerCaptor.capture());
    HttpRoutePlanner capturedRoutePlanner = routePlannerCaptor.getValue();

    HttpGet request = new HttpGet(testTargetURI);

    // Determine route should throw an error as the fake service URI is not set
    assertThrows(
        IllegalArgumentException.class,
        () ->
            capturedRoutePlanner.determineRoute(
                HttpHost.create(request.getURI().toString()), request, null));
  }

  @Test
  public void testSetFakeServiceRouteInHttpClientThrowsHTTPError() {
    // Invalid scheme
    final String testTargetURI = "invalid://example.com";
    final String testFakeServiceURI = "http://localhost:8080";
    System.setProperty(testTargetURI + FAKE_SERVICE_URI_PROP_SUFFIX, testFakeServiceURI);

    ArgumentCaptor<HttpRoutePlanner> routePlannerCaptor =
        ArgumentCaptor.forClass(HttpRoutePlanner.class);

    DatabricksHttpClient.setFakeServiceRouteInHttpClient(httpClientBuilder);

    Mockito.verify(httpClientBuilder).setRoutePlanner(routePlannerCaptor.capture());
    HttpRoutePlanner capturedRoutePlanner = routePlannerCaptor.getValue();

    HttpGet request = new HttpGet(testTargetURI);

    // Determine route should throw HTTP error as the target URI is invalid
    assertThrows(
        HttpException.class,
        () ->
            capturedRoutePlanner.determineRoute(
                HttpHost.create(request.getURI().toString()), request, null));

    System.clearProperty(testTargetURI + FAKE_SERVICE_URI_PROP_SUFFIX);
  }

  @Test
  void testExecuteThrowsError() throws IOException {
    DatabricksHttpClient databricksHttpClient =
        new DatabricksHttpClient(mockHttpClient, connectionManager);
    when(request.getURI()).thenReturn(URI.create("https://databricks.com"));
    when(mockHttpClient.execute(request)).thenThrow(new IOException());
    assertThrows(DatabricksHttpException.class, () -> databricksHttpClient.execute(request));
  }

  @Test
  void testRetryHandlerWithTemporarilyUnavailableRetryInterval() throws IOException {
    DatabricksHttpClient databricksHttpClient =
        new DatabricksHttpClient(mockHttpClient, connectionManager);
    when(request.getURI()).thenReturn(URI.create("TestURI"));
    when(mockHttpClient.execute(request))
        .thenThrow(new DatabricksRetryHandlerException("Retry http request.Error code: ", 503));
    assertThrows(DatabricksHttpException.class, () -> databricksHttpClient.execute(request));
  }

  @Test
  void testExecute() throws IOException, DatabricksHttpException {
    DatabricksHttpClient databricksHttpClient =
        new DatabricksHttpClient(mockHttpClient, connectionManager);
    when(request.getURI()).thenReturn(URI.create("TestURI"));
    when(mockHttpClient.execute(request)).thenReturn(closeableHttpResponse);
    assertEquals(closeableHttpResponse, databricksHttpClient.execute(request));
  }

  @Test
  void testCloseExpiredAndIdleConnections() {
    DatabricksHttpClient databricksHttpClient =
        new DatabricksHttpClient(mockHttpClient, connectionManager);
    databricksHttpClient.closeExpiredAndIdleConnections();
    verify(connectionManager).closeExpiredConnections();
    verify(connectionManager)
        .closeIdleConnections(DatabricksHttpClient.idleHttpConnectionExpiry, TimeUnit.SECONDS);
  }

  @Test
  void testCloseExpiredAndIdleConnectionsForNull() {
    DatabricksHttpClient databricksHttpClient = new DatabricksHttpClient(mockHttpClient, null);
    assertDoesNotThrow(databricksHttpClient::closeExpiredAndIdleConnections);
  }

  @Test
  void testUserAgent() throws Exception {
    // Thrift
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(CLUSTER_JDBC_URL, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    String userAgent = DatabricksHttpClient.getUserAgent();
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/0.9.5-oss"));
    assertTrue(userAgent.contains(" Java/THttpClient-HC-MyApp"));
    assertTrue(userAgent.contains(" databricks-jdbc-http "));
    assertFalse(userAgent.contains("databricks-sdk-java"));

    // SEA
    connectionContext = DatabricksConnectionContextFactory.create(DBSQL_JDBC_URL, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    userAgent = DatabricksHttpClient.getUserAgent();
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/0.9.5-oss"));
    assertTrue(userAgent.contains(" Java/SQLExecHttpClient-HC-MyApp"));
    assertTrue(userAgent.contains(" databricks-jdbc-http "));
    assertFalse(userAgent.contains("databricks-sdk-java"));
  }

  @Test
  public void testDifferentInstancesForDifferentContexts() {
    System.setProperty(IS_FAKE_SERVICE_TEST_PROP, "true");

    // Create the first mock connection context
    IDatabricksConnectionContext connectionContext1 =
        Mockito.mock(IDatabricksConnectionContext.class);
    when(connectionContext1.getUseProxy()).thenReturn(false);
    when(connectionContext1.getUseCloudFetchProxy()).thenReturn(false);

    // Create the second mock connection context
    IDatabricksConnectionContext connectionContext2 =
        Mockito.mock(IDatabricksConnectionContext.class);
    when(connectionContext2.getUseProxy()).thenReturn(false);
    when(connectionContext2.getUseCloudFetchProxy()).thenReturn(false);

    // Get instances of DatabricksHttpClient for each context
    DatabricksHttpClient client1 = DatabricksHttpClient.getInstance(connectionContext1);
    DatabricksHttpClient client2 = DatabricksHttpClient.getInstance(connectionContext2);

    assertNotNull(client1);
    assertNotNull(client2);

    // Assert that the instances are different for different contexts
    assertNotSame(client1, client2);

    // Reset the instance for the first context
    DatabricksHttpClient.removeInstance(connectionContext1);

    // Get a new instance for the first context
    DatabricksHttpClient newClient1 = DatabricksHttpClient.getInstance(connectionContext1);

    assertNotNull(newClient1);
    // The new instance should be different after reset
    assertNotSame(client1, newClient1);

    // Ensure that the second context's instance remains the same
    DatabricksHttpClient sameClient2 = DatabricksHttpClient.getInstance(connectionContext2);
    assertSame(client2, sameClient2);

    System.clearProperty(IS_FAKE_SERVICE_TEST_PROP);
  }
}
