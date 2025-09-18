package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.RequestType;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.HeaderFactory;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AzureExternalBrowserProviderTest {

  @Mock IDatabricksConnectionContext mockContext;
  @Mock DatabricksConfig mockDatabricksConfig;
  @Mock IDatabricksHttpClient mockHttpClient;
  @Mock CloseableHttpResponse mockConfigResponse;
  @Mock CloseableHttpResponse mockTokenResponse;

  @Test
  void testAuthType() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      lenient().when(mockFactory.getClient(any())).thenReturn(mockHttpClient);

      when(mockContext.getHost()).thenReturn("adb-foo.azuredatabricks.net");
      when(mockContext.getClientId()).thenReturn("client-id");

      AzureExternalBrowserProvider provider = new AzureExternalBrowserProvider(mockContext, 8020);
      assertEquals("azure-oauth-u2m", provider.authType());
    }
  }

  @ParameterizedTest
  @MethodSource("refreshSuccessCases")
  void testConfigure_RefreshFlowSuccessVariants(
      String tokenJson,
      String initialRefreshToken,
      String expectedRefreshToken,
      boolean setInvalidExistingAccessToken,
      String expectedAccessToken)
      throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      lenient().when(mockFactory.getClient(any())).thenReturn(mockHttpClient);

      when(mockContext.getHost()).thenReturn("adb-foo.azuredatabricks.net");
      when(mockContext.getClientId()).thenReturn("client-id");

      String configJson =
          "{\n"
              + "  \"authorization_endpoint\": \"https://login.microsoftonline.com/tenant/oauth2/v2.0/authorize\",\n"
              + "  \"token_endpoint\": \"https://login.microsoftonline.com/tenant/oauth2/v2.0/token\",\n"
              + "  \"issuer\": \"https://sts.windows.net/tenant/\"\n"
              + "}";
      when(mockConfigResponse.getEntity())
          .thenReturn(new StringEntity(configJson, StandardCharsets.UTF_8));
      when(mockTokenResponse.getEntity())
          .thenReturn(new StringEntity(tokenJson, StandardCharsets.UTF_8));

      when(mockHttpClient.executeWithRetry(any(), eq(RequestType.AUTH)))
          .thenReturn(mockConfigResponse)
          .thenReturn(mockTokenResponse);

      AzureExternalBrowserProvider provider = new AzureExternalBrowserProvider(mockContext, 8020);
      setPrivateField(provider, "refreshToken", initialRefreshToken);
      setPrivateField(provider, "accessToken", null);
      if (setInvalidExistingAccessToken) {
        setPrivateField(provider, "accessToken", "not-a-jwt");
      }

      HeaderFactory headerFactory = provider.configure(mockDatabricksConfig);
      Map<String, String> headers = headerFactory.headers();

      String authHeader = headers.get(HttpHeaders.AUTHORIZATION);
      assertNotNull(authHeader);
      assertTrue(authHeader.startsWith("Bearer "));
      assertEquals("Bearer " + expectedAccessToken, authHeader);

      String rt = (String) getPrivateField(provider, "refreshToken");
      assertEquals(expectedRefreshToken, rt);

      verify(mockHttpClient, times(2)).executeWithRetry(any(), eq(RequestType.AUTH));
    }
  }

  @Test
  void testConfigure_RefreshFlowErrorFromServer() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      lenient().when(mockFactory.getClient(any())).thenReturn(mockHttpClient);

      when(mockContext.getHost()).thenReturn("adb-foo.azuredatabricks.net");
      when(mockContext.getClientId()).thenReturn("client-id");

      String configJson =
          "{\n"
              + "  \"authorization_endpoint\": \"https://login.microsoftonline.com/tenant/oauth2/v2.0/authorize\",\n"
              + "  \"token_endpoint\": \"https://login.microsoftonline.com/tenant/oauth2/v2.0/token\",\n"
              + "  \"issuer\": \"https://sts.windows.net/tenant/\"\n"
              + "}";
      when(mockConfigResponse.getEntity())
          .thenReturn(new StringEntity(configJson, StandardCharsets.UTF_8));

      String errorJson = "{\n  \"error\": \"invalid_grant\"\n}";
      when(mockTokenResponse.getEntity())
          .thenReturn(new StringEntity(errorJson, StandardCharsets.UTF_8));

      when(mockHttpClient.executeWithRetry(any(), eq(RequestType.AUTH)))
          .thenReturn(mockConfigResponse)
          .thenReturn(mockTokenResponse);

      AzureExternalBrowserProvider provider = new AzureExternalBrowserProvider(mockContext, 8020);
      setPrivateField(provider, "refreshToken", "refresh-token");
      setPrivateField(provider, "accessToken", null);

      assertThrows(
          com.databricks.jdbc.exception.DatabricksDriverException.class,
          () -> provider.configure(mockDatabricksConfig).headers());
    }
  }

  @Test
  void testConfigure_ValidExistingAccessTokenAvoidsHttpCalls() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      lenient().when(mockFactory.getClient(any())).thenReturn(mockHttpClient);

      when(mockContext.getHost()).thenReturn("adb-foo.azuredatabricks.net");
      when(mockContext.getClientId()).thenReturn("client-id");

      AzureExternalBrowserProvider provider = new AzureExternalBrowserProvider(mockContext, 8020);
      String jwt = createUnsignedJwtWithExp(Instant.now().plusSeconds(3600).getEpochSecond());
      setPrivateField(provider, "accessToken", jwt);

      Map<String, String> headers = provider.configure(mockDatabricksConfig).headers();
      assertEquals("Bearer " + jwt, headers.get(HttpHeaders.AUTHORIZATION));
      verify(mockHttpClient, never()).executeWithRetry(any(), eq(RequestType.AUTH));
    }
  }

  @Test
  void testConfigure_ConfigDiscoveryMissingIssuerThrows() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      lenient().when(mockFactory.getClient(any())).thenReturn(mockHttpClient);

      when(mockContext.getHost()).thenReturn("adb-foo.azuredatabricks.net");
      when(mockContext.getClientId()).thenReturn("client-id");

      String configJson =
          "{\n"
              + "  \"authorization_endpoint\": \"https://login.microsoftonline.com/tenant/oauth2/v2.0/authorize\",\n"
              + "  \"token_endpoint\": \"https://login.microsoftonline.com/tenant/oauth2/v2.0/token\"\n"
              + "}"; // missing issuer
      when(mockConfigResponse.getEntity())
          .thenReturn(new StringEntity(configJson, StandardCharsets.UTF_8));

      when(mockHttpClient.executeWithRetry(any(), eq(RequestType.AUTH)))
          .thenReturn(mockConfigResponse);

      AzureExternalBrowserProvider provider = new AzureExternalBrowserProvider(mockContext, 8020);
      setPrivateField(provider, "refreshToken", "refresh-token");
      setPrivateField(provider, "accessToken", null);

      assertThrows(
          com.databricks.jdbc.exception.DatabricksDriverException.class,
          () -> provider.configure(mockDatabricksConfig).headers());
    }
  }

  private static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments>
      refreshSuccessCases() {
    String jwt = createUnsignedJwtWithExp(Instant.now().plusSeconds(1800).getEpochSecond());
    String case1 =
        "{\n"
            + "  \"access_token\": \"new-access\",\n"
            + "  \"refresh_token\": \"new-refresh\",\n"
            + "  \"expires_in\": 3600\n"
            + "}";
    String case2 =
        "{\n" + "  \"access_token\": \"new-access\",\n" + "  \"expires_in\": 3600\n" + "}";
    String case3 = "{\n" + "  \"access_token\": \"" + jwt + "\"\n" + "}";
    return java.util.stream.Stream.of(
        org.junit.jupiter.params.provider.Arguments.of(
            case1, "refresh-token", "new-refresh", false, "new-access"),
        org.junit.jupiter.params.provider.Arguments.of(
            case2, "old-refresh", "old-refresh", false, "new-access"),
        org.junit.jupiter.params.provider.Arguments.of(
            case3, "refresh-token", "refresh-token", false, jwt),
        org.junit.jupiter.params.provider.Arguments.of(
            case1, "refresh-token", "new-refresh", true, "new-access"));
  }

  @Test
  void testPerformOAuthFlow_StateMismatch() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      String prevHeadless = System.getProperty("java.awt.headless");
      System.setProperty("java.awt.headless", "true");
      try {
        DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
        factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
        lenient().when(mockFactory.getClient(any())).thenReturn(mockHttpClient);

        when(mockContext.getHost()).thenReturn("adb-foo.azuredatabricks.net");
        when(mockContext.getClientId()).thenReturn("client-id");

        String configJson =
            "{\n"
                + "  \"authorization_endpoint\": \"https://login.microsoftonline.com/tenant/oauth2/v2.0/authorize\",\n"
                + "  \"token_endpoint\": \"https://login.microsoftonline.com/tenant/oauth2/v2.0/token\",\n"
                + "  \"issuer\": \"https://sts.windows.net/tenant/\"\n"
                + "}";
        when(mockConfigResponse.getEntity())
            .thenReturn(new StringEntity(configJson, StandardCharsets.UTF_8));
        when(mockHttpClient.executeWithRetry(any(), eq(RequestType.AUTH)))
            .thenReturn(mockConfigResponse);

        AzureExternalBrowserProvider provider = new AzureExternalBrowserProvider(mockContext, 0);

        // Kick off performOAuthFlow on a background thread
        java.util.concurrent.CompletableFuture<Void> flowFuture =
            java.util.concurrent.CompletableFuture.runAsync(
                () -> {
                  try {
                    invokePrivate(provider, "performOAuthFlow");
                    fail("Expected DatabricksDriverException for state mismatch");
                  } catch (Exception ignored) {
                    // expected
                  }
                });

        // Wait for server to be ready and obtain bound port
        Object server = getPrivateField(provider, "callbackServer");
        waitUntilTrue(() -> (boolean) getPrivateField(server, "isReady"), 5000);
        int actualPort =
            ((java.net.ServerSocket) getPrivateField(server, "serverSocket")).getLocalPort();

        // Send callback with mismatched state
        try (java.net.Socket socket = new java.net.Socket("127.0.0.1", actualPort)) {
          java.io.OutputStream os = socket.getOutputStream();
          String request = "GET /?code=abc&state=wrong HTTP/1.1\r\nHost: localhost\r\n\r\n";
          os.write(request.getBytes(StandardCharsets.UTF_8));
          os.flush();

          // Optionally read response to ensure sendSuccessResponse ran
          java.io.BufferedReader br =
              new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));
          String statusLine = br.readLine();
          assertEquals("HTTP/1.1 200 OK", statusLine);
        }

        // Await flow termination (should throw inside thread and complete)
        flowFuture.get(5, java.util.concurrent.TimeUnit.SECONDS);
      } finally {
        if (prevHeadless == null) System.clearProperty("java.awt.headless");
        else System.setProperty("java.awt.headless", prevHeadless);
      }
    }
  }

  @Test
  void testPerformOAuthFlow_ErrorParameter() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      String prevHeadless = System.getProperty("java.awt.headless");
      System.setProperty("java.awt.headless", "true");
      try {
        DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
        factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
        lenient().when(mockFactory.getClient(any())).thenReturn(mockHttpClient);

        when(mockContext.getHost()).thenReturn("adb-foo.azuredatabricks.net");
        when(mockContext.getClientId()).thenReturn("client-id");

        String configJson =
            "{\n"
                + "  \"authorization_endpoint\": \"https://login.microsoftonline.com/tenant/oauth2/v2.0/authorize\",\n"
                + "  \"token_endpoint\": \"https://login.microsoftonline.com/tenant/oauth2/v2.0/token\",\n"
                + "  \"issuer\": \"https://sts.windows.net/tenant/\"\n"
                + "}";
        when(mockConfigResponse.getEntity())
            .thenReturn(new StringEntity(configJson, StandardCharsets.UTF_8));
        when(mockHttpClient.executeWithRetry(any(), eq(RequestType.AUTH)))
            .thenReturn(mockConfigResponse);

        AzureExternalBrowserProvider provider = new AzureExternalBrowserProvider(mockContext, 0);

        java.util.concurrent.CompletableFuture<Void> flowFuture =
            java.util.concurrent.CompletableFuture.runAsync(
                () -> {
                  try {
                    invokePrivate(provider, "performOAuthFlow");
                    fail("Expected error due to OAuth error parameter");
                  } catch (Exception ignored) {
                  }
                });

        Object server = getPrivateField(provider, "callbackServer");
        waitUntilTrue(() -> (boolean) getPrivateField(server, "isReady"), 5000);
        int actualPort =
            ((java.net.ServerSocket) getPrivateField(server, "serverSocket")).getLocalPort();

        try (java.net.Socket socket = new java.net.Socket("127.0.0.1", actualPort)) {
          java.io.OutputStream os = socket.getOutputStream();
          String request =
              "GET /?error=access_denied&state=whatever HTTP/1.1\r\nHost: localhost\r\n\r\n";
          os.write(request.getBytes(StandardCharsets.UTF_8));
          os.flush();

          java.io.BufferedReader br =
              new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));
          String statusLine = br.readLine();
          assertEquals("HTTP/1.1 200 OK", statusLine);
        }

        flowFuture.get(5, java.util.concurrent.TimeUnit.SECONDS);
      } finally {
        if (prevHeadless == null) System.clearProperty("java.awt.headless");
        else System.setProperty("java.awt.headless", prevHeadless);
      }
    }
  }

  @Test
  void testOAuthCallbackServer_InvalidRequest_Sends400() throws Exception {
    Object server =
        newInner("com.databricks.jdbc.auth.AzureExternalBrowserProvider$OAuthCallbackServer");
    // start on ephemeral port 0
    invokePrivate(server, "start", 0);
    int port = ((java.net.ServerSocket) getPrivateField(server, "serverSocket")).getLocalPort();
    try (java.net.Socket socket = new java.net.Socket("127.0.0.1", port)) {
      java.io.OutputStream os = socket.getOutputStream();
      String bad = "BADREQUEST\r\n\r\n";
      os.write(bad.getBytes(StandardCharsets.UTF_8));
      os.flush();

      java.io.BufferedReader br =
          new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));
      String statusLine = br.readLine();
      assertEquals("HTTP/1.1 400 Bad Request", statusLine);
    } finally {
      invokePrivate(server, "stop");
    }
  }

  @Test
  void testOAuthCallbackServer_parseQueryString() throws Exception {
    Object server =
        newInner("com.databricks.jdbc.auth.AzureExternalBrowserProvider$OAuthCallbackServer");
    @SuppressWarnings("unchecked")
    Map<String, String> params =
        (Map<String, String>)
            invokePrivate(server, "parseQueryString", "code=a%2Bb&state=st%20ate&error=e%2Fr");
    assertEquals("a+b", params.get("code"));
    assertEquals("st ate", params.get("state"));
    assertEquals("e/r", params.get("error"));
  }

  @ParameterizedTest
  @MethodSource("exchangeCodeCases")
  void testExchangeCodeForTokens(String responseJson, boolean expectError, String expectedAccess)
      throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(mockHttpClient);

      when(mockContext.getHost()).thenReturn("adb-foo.azuredatabricks.net");
      when(mockContext.getClientId()).thenReturn("client-id");

      AzureExternalBrowserProvider provider = new AzureExternalBrowserProvider(mockContext, 8021);
      Object oauthConfig =
          newInner(
              "com.databricks.jdbc.auth.AzureExternalBrowserProvider$OAuthConfig",
              "https://auth",
              "https://token",
              "issuer");
      setPrivateField(provider, "oauthConfig", oauthConfig);

      when(mockTokenResponse.getEntity())
          .thenReturn(new StringEntity(responseJson, StandardCharsets.UTF_8));
      when(mockHttpClient.executeWithRetry(any(), eq(RequestType.AUTH)))
          .thenReturn(mockTokenResponse);

      Executable call =
          () -> {
            try {
              invokePrivate(
                  provider, "exchangeCodeForTokens", "code", "verifier", "http://localhost");
            } catch (java.lang.reflect.InvocationTargetException ite) {
              Throwable c = ite.getCause();
              if (c instanceof RuntimeException) throw (RuntimeException) c;
              if (c instanceof Error) throw (Error) c;
              throw new RuntimeException(c);
            }
          };
      if (expectError) {
        assertThrows(com.databricks.jdbc.exception.DatabricksDriverException.class, call);
      } else {
        assertDoesNotThrow(call);
        assertEquals(
            "Bearer " + expectedAccess,
            provider.configure(mockDatabricksConfig).headers().get(HttpHeaders.AUTHORIZATION));
      }
    }
  }

  private static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments>
      exchangeCodeCases() {
    String ok =
        "{\n  \"access_token\": \"ok-token\",\n  \"refresh_token\": \"rt\",\n  \"expires_in\": 10\n}";
    String err = "{\n  \"error\": \"invalid_request\"\n}";
    return java.util.stream.Stream.of(
        org.junit.jupiter.params.provider.Arguments.of(ok, false, "ok-token"),
        org.junit.jupiter.params.provider.Arguments.of(err, true, null));
  }

  @Test
  void testBuildAuthorizationUrlAndGenerateRandomStringAndConfigGetters() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(mockHttpClient);

      when(mockContext.getHost()).thenReturn("adb-foo.azuredatabricks.net");
      when(mockContext.getClientId()).thenReturn("client-id");

      AzureExternalBrowserProvider provider = new AzureExternalBrowserProvider(mockContext, 8022);
      Object oauthConfig =
          newInner(
              "com.databricks.jdbc.auth.AzureExternalBrowserProvider$OAuthConfig",
              "https://auth",
              "https://token",
              "issuer");
      setPrivateField(provider, "oauthConfig", oauthConfig);

      Object pkce = invokePrivate(provider, "generatePKCEChallenge");
      String verifier = (String) invokePrivate(pkce, "getVerifier");
      String challenge = (String) invokePrivate(pkce, "getChallenge");
      assertNotNull(verifier);
      assertNotNull(challenge);

      String state = (String) invokePrivate(provider, "generateRandomString", 16);
      assertNotNull(state);
      assertTrue(state.matches("[A-Za-z0-9_-]+"));

      String url =
          (String)
              invokePrivate(
                  provider, "buildAuthorizationUrl", pkce, "http://localhost:1234", "state-123");
      assertTrue(url.contains("response_type=code"));
      assertTrue(url.contains("client_id=client-id"));
      assertTrue(url.contains("scope=offline_access+sql"));
      assertTrue(url.contains("redirect_uri=http%3A%2F%2Flocalhost%3A1234"));
      assertTrue(url.contains("state=state-123"));
      assertTrue(url.contains("code_challenge_method=S256"));

      assertEquals("https://auth", invokePrivate(oauthConfig, "getAuthorizationEndpoint"));
      assertEquals("https://token", invokePrivate(oauthConfig, "getTokenEndpoint"));
      assertEquals("issuer", invokePrivate(oauthConfig, "getIssuer"));
    }
  }

  @Test
  void testGeneratePKCEChallengeAndBuildAuthorizationUrl_Params() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(mockHttpClient);

      when(mockContext.getHost()).thenReturn("adb-foo.azuredatabricks.net");
      when(mockContext.getClientId()).thenReturn("client-id");

      AzureExternalBrowserProvider provider = new AzureExternalBrowserProvider(mockContext, 8023);
      Object oauthConfig =
          newInner(
              "com.databricks.jdbc.auth.AzureExternalBrowserProvider$OAuthConfig",
              "https://auth",
              "https://token",
              "issuer");
      setPrivateField(provider, "oauthConfig", oauthConfig);

      Object pkce = invokePrivate(provider, "generatePKCEChallenge");
      String verifier = (String) invokePrivate(pkce, "getVerifier");
      String challenge = (String) invokePrivate(pkce, "getChallenge");

      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      String expectedChallenge =
          Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString(digest.digest(verifier.getBytes(StandardCharsets.UTF_8)));
      assertEquals(expectedChallenge, challenge);

      String redirect = "http://localhost:12345";
      String state = "stateXYZ";
      String url = (String) invokePrivate(provider, "buildAuthorizationUrl", pkce, redirect, state);
      assertTrue(url.startsWith("https://auth"));

      URI uri = URI.create(url);
      String[] pairs = uri.getQuery().split("&");
      java.util.HashMap<String, String> params = new java.util.HashMap<>();
      for (String p : pairs) {
        String[] kv = p.split("=", 2);
        params.put(
            java.net.URLDecoder.decode(kv[0], StandardCharsets.UTF_8),
            java.net.URLDecoder.decode(kv[1], StandardCharsets.UTF_8));
      }
      assertEquals("code", params.get("response_type"));
      assertEquals("client-id", params.get("client_id"));
      assertEquals("offline_access sql", params.get("scope"));
      assertEquals(redirect, params.get("redirect_uri"));
      assertEquals(state, params.get("state"));
      assertEquals(challenge, params.get("code_challenge"));
      assertEquals("S256", params.get("code_challenge_method"));
    }
  }

  // ---------- helpers ----------
  private static void waitUntilTrue(java.util.concurrent.Callable<Boolean> cond, long timeoutMs)
      throws Exception {
    long start = System.currentTimeMillis();
    while (!cond.call()) {
      if (System.currentTimeMillis() - start > timeoutMs) {
        throw new AssertionError("Condition not met within timeout");
      }
      Thread.sleep(25);
    }
  }

  private static Object newInner(String className, Object... ctorArgs) throws Exception {
    Class<?> cls = Class.forName(className);
    if (ctorArgs.length == 0) {
      java.lang.reflect.Constructor<?> c = cls.getDeclaredConstructor();
      c.setAccessible(true);
      return c.newInstance();
    }
    Class<?>[] types = new Class<?>[ctorArgs.length];
    for (int i = 0; i < ctorArgs.length; i++) types[i] = String.class;
    java.lang.reflect.Constructor<?> c = cls.getDeclaredConstructor(types);
    c.setAccessible(true);
    return c.newInstance(ctorArgs);
  }

  private static Object invokePrivate(Object targetOrClass, String methodName, Object... args)
      throws Exception {
    Class<?> cls =
        (targetOrClass instanceof Class) ? (Class<?>) targetOrClass : targetOrClass.getClass();
    Class<?>[] types = new Class<?>[args.length];
    for (int i = 0; i < args.length; i++) {
      Object a = args[i];
      if (a instanceof Integer) types[i] = int.class;
      else if (a instanceof Long) types[i] = long.class;
      else if (a instanceof Boolean) types[i] = boolean.class;
      else if (a instanceof Byte) types[i] = byte.class;
      else if (a instanceof Short) types[i] = short.class;
      else if (a instanceof Float) types[i] = float.class;
      else if (a instanceof Double) types[i] = double.class;
      else if (a instanceof Character) types[i] = char.class;
      else types[i] = a.getClass();
    }
    try {
      java.lang.reflect.Method m = cls.getDeclaredMethod(methodName, types);
      m.setAccessible(true);
      return m.invoke((targetOrClass instanceof Class) ? null : targetOrClass, args);
    } catch (NoSuchMethodException e) {
      // Fallback: match by name and parameter count
      for (java.lang.reflect.Method cand : cls.getDeclaredMethods()) {
        if (!cand.getName().equals(methodName)) continue;
        if (cand.getParameterCount() != args.length) continue;
        cand.setAccessible(true);
        return cand.invoke((targetOrClass instanceof Class) ? null : targetOrClass, args);
      }
      throw e;
    }
  }

  private static Object getPrivateField(Object target, String fieldName) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(target);
  }

  private static String createUnsignedJwtWithExp(long epochSeconds) {
    String header =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString("{\"alg\":\"HS256\"}".getBytes(StandardCharsets.UTF_8));
    String payload =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(("{\"exp\":" + epochSeconds + "}").getBytes(StandardCharsets.UTF_8));
    String signature =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString("sig".getBytes(StandardCharsets.UTF_8));
    return header + "." + payload + "." + signature;
  }

  private static void setPrivateField(Object target, String fieldName, Object value)
      throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
