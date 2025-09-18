package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.RequestType;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.HeaderFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.http.HttpHeaders;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

/**
 * Production-ready Azure U2M OAuth provider for Databricks SQL.
 *
 * <p>This provider implements the OAuth 2.0 Authorization Code flow with PKCE (Proof Key for Code
 * Exchange) for Azure Databricks workspaces. It handles: - OAuth configuration discovery - PKCE
 * challenge generation - Browser-based authentication - Token exchange and refresh - Automatic
 * token expiration handling
 */
public class AzureExternalBrowserProvider implements CredentialsProvider {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(AzureExternalBrowserProvider.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // OAuth constants
  private static final String DEFAULT_SCOPE = "offline_access sql";
  private static final String CODE_CHALLENGE_METHOD = "S256";
  private static final String GRANT_TYPE_AUTHORIZATION_CODE = "authorization_code";
  private static final String GRANT_TYPE_REFRESH_TOKEN = "refresh_token";

  // HTTP constants
  private static final String CONTENT_TYPE_FORM_URLENCODED = "application/x-www-form-urlencoded";
  private static final String ACCEPT_JSON = "application/json";

  // Default port range for OAuth callback
  private static final List<Integer> DEFAULT_PORT_RANGE = List.of(8020, 8021, 8022, 8023, 8024);

  private final String hostname;
  private final String clientId;
  private final IDatabricksConnectionContext connectionContext;
  private final IDatabricksHttpClient httpClient;
  private final OAuthCallbackServer callbackServer;
  private final int callbackPort;

  private String accessToken;
  private String refreshToken;
  private LocalDateTime tokenExpiry;
  private OAuthConfig oauthConfig;

  /**
   * Constructor for Azure OAuth provider.
   *
   * @param connectionContext The connection context containing OAuth configuration
   */
  public AzureExternalBrowserProvider(
      IDatabricksConnectionContext connectionContext, int availablePort)
      throws DatabricksParsingException {
    this.connectionContext = connectionContext;
    this.hostname = connectionContext.getHost();
    this.clientId = connectionContext.getClientId();
    this.httpClient = DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
    this.callbackServer = new OAuthCallbackServer();
    this.callbackPort = availablePort;
  }

  @Override
  public String authType() {
    return "azure-oauth-u2m";
  }

  @Override
  public HeaderFactory configure(DatabricksConfig databricksConfig) {
    return () -> {
      ensureValidTokens();
      Map<String, String> headers = new HashMap<>();
      headers.put(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
      return headers;
    };
  }

  /** Ensures that valid tokens are available. */
  private void ensureValidTokens() {
    if (accessToken == null || isTokenExpired(accessToken)) {
      try {
        if (refreshToken != null) {
          // Try to refresh the token first
          try {
            refreshAccessToken();
            return;
          } catch (Exception e) {
            LOGGER.warn("Token refresh failed, re-authenticating: {}", e.getMessage());
          }
        }
        // Perform full OAuth flow if refresh failed or no refresh token
        performOAuthFlow();
      } catch (Exception e) {
        LOGGER.error(e, "Failed to obtain OAuth tokens");
        throw new DatabricksDriverException(
            "Failed to obtain OAuth tokens", e, DatabricksDriverErrorCode.AUTH_ERROR);
      }
    }
  }

  /** Refreshes the access token using the refresh token. */
  private void refreshAccessToken() throws Exception {
    if (refreshToken == null) {
      throw new DatabricksDriverException(
          "No refresh token available", DatabricksDriverErrorCode.AUTH_ERROR);
    }

    if (oauthConfig == null) {
      // Fetch OAuth configuration if not already available
      oauthConfig = fetchOAuthConfig();
    }

    HttpPost request = new HttpPost(oauthConfig.getTokenEndpoint());
    request.setHeader(HttpHeaders.CONTENT_TYPE, CONTENT_TYPE_FORM_URLENCODED);
    request.setHeader(HttpHeaders.ACCEPT, ACCEPT_JSON);

    List<BasicNameValuePair> params =
        List.of(
            new BasicNameValuePair("grant_type", GRANT_TYPE_REFRESH_TOKEN),
            new BasicNameValuePair("client_id", clientId),
            new BasicNameValuePair("refresh_token", refreshToken));

    request.setEntity(new UrlEncodedFormEntity(params));

    try (CloseableHttpResponse response = httpClient.executeWithRetry(request, RequestType.AUTH)) {
      String responseBody = EntityUtils.toString(response.getEntity());
      JsonNode tokenResponse = OBJECT_MAPPER.readTree(responseBody);

      if (tokenResponse.has("error")) {
        String error = tokenResponse.get("error").asText();
        throw new DatabricksDriverException(
            "Token refresh failed: " + error, DatabricksDriverErrorCode.AUTH_ERROR);
      }

      this.accessToken = tokenResponse.get("access_token").asText();
      // Update refresh token if a new one is provided
      if (tokenResponse.has("refresh_token")) {
        this.refreshToken = tokenResponse.get("refresh_token").asText();
      }

      if (tokenResponse.has("expires_in")) {
        int expiresIn = tokenResponse.get("expires_in").asInt();
        this.tokenExpiry = LocalDateTime.now().plusSeconds(expiresIn);
      } else {
        this.tokenExpiry = parseTokenExpiration(accessToken);
      }

      LOGGER.debug("Successfully refreshed OAuth tokens");

    } catch (Exception e) {
      LOGGER.error(e, "Token refresh failed");
      throw new DatabricksDriverException(
          "Failed to refresh access token", e, DatabricksDriverErrorCode.AUTH_ERROR);
    }
  }

  /** Performs the complete OAuth 2.0 authorization code flow with PKCE. */
  private void performOAuthFlow() throws Exception {
    // Fetch OAuth configuration
    oauthConfig = fetchOAuthConfig();
    // Generate PKCE challenge
    PKCEChallenge pkce = generatePKCEChallenge();
    // Find available port for callback
    String redirectUri = "http://localhost:" + callbackPort;
    // Generate state parameter for security
    String state = generateRandomString(32);
    // Build authorization URL
    String authUrl = buildAuthorizationUrl(pkce, redirectUri, state);
    // Start callback server
    LOGGER.debug("Starting OAuth callback server on port {}", callbackPort);
    callbackServer.start(callbackPort);
    try {
      // Give the server a moment to be fully ready
      Thread.sleep(200);

      // Open browser for authentication
      openBrowser(authUrl);
      // Wait for callback
      OAuthCallback callback = callbackServer.waitForCallback(300, TimeUnit.SECONDS);

      // Validate state parameter
      if (!state.equals(callback.getState())) {
        String error =
            String.format(
                "OAuth state parameter mismatch. Expected: %s, Received: %s",
                state, callback.getState());
        LOGGER.error(error);
        throw new DatabricksDriverException(error, DatabricksDriverErrorCode.AUTH_ERROR);
      }
      // Exchange authorization code for tokens
      exchangeCodeForTokens(callback.getCode(), pkce.getVerifier(), redirectUri);
    } catch (Exception e) {
      String errorMessage = String.format("OAuth flow failed for Azure U2M. Error: %s", e);
      LOGGER.error(e, errorMessage);
      throw e;
    } finally {
      LOGGER.debug("Stopping OAuth callback server");
      callbackServer.stop();
    }
  }

  /** Fetches OAuth configuration from the well-known endpoint. */
  private OAuthConfig fetchOAuthConfig() {
    String configUrl =
        "https://"
            + hostname
            + "/oidc/.well-known/oauth-authorization-server"; // TODO: add discovery url here
    LOGGER.debug("Fetching OAuth configuration from: {}", configUrl);

    HttpGet request = new HttpGet(configUrl);
    request.setHeader(HttpHeaders.ACCEPT, ACCEPT_JSON);

    try (CloseableHttpResponse response = httpClient.executeWithRetry(request, RequestType.AUTH)) {
      String responseBody = EntityUtils.toString(response.getEntity());

      JsonNode config = OBJECT_MAPPER.readTree(responseBody);

      String authorizationEndpoint = config.get("authorization_endpoint").asText();
      String tokenEndpoint = config.get("token_endpoint").asText();
      String issuer = config.get("issuer").asText();
      // Validate the configuration
      validateOAuthConfig(authorizationEndpoint, tokenEndpoint, issuer);
      return new OAuthConfig(authorizationEndpoint, tokenEndpoint, issuer);
    } catch (Exception e) {
      LOGGER.error(e, "Failed to fetch OAuth config from {}", configUrl);
      throw new DatabricksDriverException(
          "Unable to fetch OAuth configuration", e, DatabricksDriverErrorCode.AUTH_ERROR);
    }
  }

  /** Validates the OAuth configuration and provides helpful error messages. */
  private void validateOAuthConfig(
      String authorizationEndpoint, String tokenEndpoint, String issuer) {
    if (authorizationEndpoint == null || authorizationEndpoint.trim().isEmpty()) {
      throw new DatabricksDriverException(
          "OAuth configuration is missing authorization endpoint",
          DatabricksDriverErrorCode.AUTH_ERROR);
    }

    if (tokenEndpoint == null || tokenEndpoint.trim().isEmpty()) {
      throw new DatabricksDriverException(
          "OAuth configuration is missing token endpoint", DatabricksDriverErrorCode.AUTH_ERROR);
    }

    if (issuer == null || issuer.trim().isEmpty()) {
      throw new DatabricksDriverException(
          "OAuth configuration is missing issuer", DatabricksDriverErrorCode.AUTH_ERROR);
    }
  }

  /** Generates PKCE challenge and verifier. */
  private PKCEChallenge generatePKCEChallenge() throws NoSuchAlgorithmException {
    SecureRandom random = new SecureRandom();
    byte[] verifierBytes = new byte[32];
    random.nextBytes(verifierBytes);
    String verifier = Base64.getUrlEncoder().withoutPadding().encodeToString(verifierBytes);

    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] challengeBytes = digest.digest(verifier.getBytes(StandardCharsets.UTF_8));
    String challenge = Base64.getUrlEncoder().withoutPadding().encodeToString(challengeBytes);

    return new PKCEChallenge(verifier, challenge);
  }

  /** Builds the authorization URL for OAuth flow. */
  private String buildAuthorizationUrl(PKCEChallenge pkce, String redirectUri, String state)
      throws Exception {
    URIBuilder builder = new URIBuilder(oauthConfig.getAuthorizationEndpoint());
    builder.addParameter("response_type", "code");
    builder.addParameter("client_id", clientId);
    builder.addParameter("scope", DEFAULT_SCOPE);
    builder.addParameter("redirect_uri", redirectUri);
    builder.addParameter("state", state);
    builder.addParameter("code_challenge", pkce.getChallenge());
    builder.addParameter("code_challenge_method", CODE_CHALLENGE_METHOD);
    return builder.build().toString();
  }

  /** Opens the default browser with the authorization URL. */
  private void openBrowser(String authUrl) {
    LOGGER.debug(
        "If the browser doesn't open automatically, please manually navigate to: {}", authUrl);

    if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
      try {
        Desktop.getDesktop().browse(new URI(authUrl));
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to open browser automatically: {}. Please manually open your browser and navigate to: {}",
            e.getMessage(),
            authUrl);
        // Don't throw an exception here, just log the warning and continue
        // The user can still manually open the browser
      }
    } else {
      LOGGER.warn(
          "Desktop browsing not supported on this platform. Please manually open your browser and navigate to: {}",
          authUrl);
      // Don't throw an exception here, just log the warning and continue
      // The user can still manually open the browser
    }
  }

  /** Exchanges authorization code for access and refresh tokens. */
  private void exchangeCodeForTokens(String code, String codeVerifier, String redirectUri)
      throws Exception {
    HttpPost request = new HttpPost(oauthConfig.getTokenEndpoint());
    request.setHeader(HttpHeaders.CONTENT_TYPE, CONTENT_TYPE_FORM_URLENCODED);
    request.setHeader(HttpHeaders.ACCEPT, ACCEPT_JSON);

    List<BasicNameValuePair> params =
        List.of(
            new BasicNameValuePair("grant_type", GRANT_TYPE_AUTHORIZATION_CODE),
            new BasicNameValuePair("client_id", clientId),
            new BasicNameValuePair("code", code),
            new BasicNameValuePair("redirect_uri", redirectUri),
            new BasicNameValuePair("code_verifier", codeVerifier));

    request.setEntity(new UrlEncodedFormEntity(params));

    try (CloseableHttpResponse response = httpClient.executeWithRetry(request, RequestType.AUTH)) {
      String responseBody = EntityUtils.toString(response.getEntity());
      JsonNode tokenResponse = OBJECT_MAPPER.readTree(responseBody);

      if (tokenResponse.has("error")) {
        String error = tokenResponse.get("error").asText();
        String errorDescription =
            tokenResponse.has("error_description")
                ? tokenResponse.get("error_description").asText()
                : error;
        throw new DatabricksDriverException(
            "OAuth token error: " + errorDescription, DatabricksDriverErrorCode.AUTH_ERROR);
      }

      this.accessToken = tokenResponse.get("access_token").asText();
      this.refreshToken =
          tokenResponse.has("refresh_token") ? tokenResponse.get("refresh_token").asText() : null;

      // Parse token expiration
      if (tokenResponse.has("expires_in")) {
        int expiresIn = tokenResponse.get("expires_in").asInt();
        this.tokenExpiry = LocalDateTime.now().plusSeconds(expiresIn);
      } else {
        // Parse from JWT if no expires_in
        this.tokenExpiry = parseTokenExpiration(accessToken);
      }

    } catch (Exception e) {
      LOGGER.error(e, "Token exchange failed");
      throw new DatabricksDriverException(
          "Failed to exchange code for tokens", e, DatabricksDriverErrorCode.AUTH_ERROR);
    }
  }

  /** Checks if a token is expired. */
  private boolean isTokenExpired(String token) {
    try {
      LocalDateTime expiration = parseTokenExpiration(token);
      return LocalDateTime.now().isAfter(expiration);
    } catch (Exception e) {
      LOGGER.warn("Could not parse token expiration: {}", e.getMessage());
      return true;
    }
  }

  /** Parses token expiration from JWT. */
  private LocalDateTime parseTokenExpiration(String token) throws ParseException {
    SignedJWT signedJWT = SignedJWT.parse(token);
    JWTClaimsSet claims = signedJWT.getJWTClaimsSet();

    if (claims.getExpirationTime() == null) {
      throw new DatabricksDriverException(
          "Token has no expiration time", DatabricksDriverErrorCode.AUTH_ERROR);
    }

    Instant expirationTime = Instant.ofEpochMilli(claims.getExpirationTime().getTime());
    return expirationTime.atZone(ZoneId.systemDefault()).toLocalDateTime();
  }

  /** Generates a random string of specified length. */
  private String generateRandomString(int length) {
    SecureRandom random = new SecureRandom();
    byte[] bytes = new byte[length];
    random.nextBytes(bytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  /** PKCE challenge container. */
  private static class PKCEChallenge {
    private final String verifier;
    private final String challenge;

    public PKCEChallenge(String verifier, String challenge) {
      this.verifier = verifier;
      this.challenge = challenge;
    }

    public String getVerifier() {
      return verifier;
    }

    public String getChallenge() {
      return challenge;
    }
  }

  /** OAuth configuration container. */
  private static class OAuthConfig {
    private final String authorizationEndpoint;
    private final String tokenEndpoint;
    private final String issuer;

    public OAuthConfig(String authorizationEndpoint, String tokenEndpoint, String issuer) {
      this.authorizationEndpoint = authorizationEndpoint;
      this.tokenEndpoint = tokenEndpoint;
      this.issuer = issuer;
    }

    public String getAuthorizationEndpoint() {
      return authorizationEndpoint;
    }

    public String getTokenEndpoint() {
      return tokenEndpoint;
    }

    public String getIssuer() {
      return issuer;
    }
  }

  /** OAuth callback container. */
  private static class OAuthCallback {
    private final String code;
    private final String state;

    public OAuthCallback(String code, String state) {
      this.code = code;
      this.state = state;
    }

    public String getCode() {
      return code;
    }

    public String getState() {
      return state;
    }
  }

  /** Simple HTTP server to handle OAuth callback. */
  private static class OAuthCallbackServer {
    private java.net.ServerSocket serverSocket;
    private CompletableFuture<OAuthCallback> callbackFuture;
    private volatile boolean isReady = false;
    private final Object serverLock = new Object();

    public void start(int port) throws IOException {
      synchronized (serverLock) {
        serverSocket = new java.net.ServerSocket(port);
        serverSocket.setReuseAddress(true);
        serverSocket.setSoTimeout(300000); // 5 minute timeout
        callbackFuture = new CompletableFuture<>();

        Thread serverThread =
            new Thread(
                () -> {
                  try {
                    synchronized (serverLock) {
                      isReady = true;
                      serverLock.notifyAll();
                    }
                    LOGGER.debug("OAuth callback server started and ready on port {}", port);

                    java.net.Socket clientSocket = serverSocket.accept();
                    LOGGER.debug(
                        "OAuth callback connection accepted from {}",
                        clientSocket.getInetAddress());
                    handleCallback(clientSocket);
                  } catch (IOException e) {
                    if (!serverSocket.isClosed()) {
                      LOGGER.error(e, "Error handling OAuth callback");
                      callbackFuture.completeExceptionally(e);
                    }
                  }
                });
        serverThread.setDaemon(true);
        serverThread.start();

        // Wait for server to be ready
        synchronized (serverLock) {
          while (!isReady) {
            try {
              serverLock.wait(1000); // Wait up to 1 second
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException("Server startup interrupted", e);
            }
          }
        }

        // Additional verification that server is listening
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Server startup interrupted", e);
        }

        LOGGER.debug("OAuth callback server is ready and listening on port {}", port);
      }
    }

    private void handleCallback(java.net.Socket clientSocket) throws IOException {
      try (java.io.BufferedReader reader =
              new java.io.BufferedReader(
                  new java.io.InputStreamReader(clientSocket.getInputStream()));
          java.io.PrintWriter writer = new java.io.PrintWriter(clientSocket.getOutputStream())) {

        // Read HTTP request
        String requestLine = reader.readLine();
        if (requestLine == null) {
          LOGGER.warn("Received empty request from OAuth callback");
          return;
        }

        LOGGER.debug("Received OAuth callback request: {}", requestLine);

        // Parse query parameters
        String[] parts = requestLine.split(" ");
        if (parts.length < 2) {
          LOGGER.warn("Invalid OAuth callback request format: {}", requestLine);
          sendErrorResponse(writer, "Invalid request");
          return;
        }

        String query = parts[1];
        if (query.startsWith("/?")) {
          query = query.substring(2);
        } else if (query.equals("/")) {
          // Handle root path requests
          LOGGER.debug("Received root path request, sending success page");
          sendSuccessResponse(writer);
          return;
        }

        Map<String, String> params = parseQueryString(query);
        String code = params.get("code");
        String state = params.get("state");
        String error = params.get("error");

        LOGGER.debug(
            "OAuth callback parameters - code: {}, state: {}, error: {}",
            code != null ? "present" : "null",
            state != null ? "present" : "null",
            error);

        // Send response
        sendSuccessResponse(writer);

        if (error != null) {
          LOGGER.error("OAuth error received: {}", error);
          callbackFuture.completeExceptionally(
              new DatabricksDriverException(
                  "OAuth error: " + error, DatabricksDriverErrorCode.AUTH_ERROR));
        } else if (code != null) {
          LOGGER.debug("OAuth authorization code received successfully");
          callbackFuture.complete(new OAuthCallback(code, state));
        } else {
          LOGGER.error("No authorization code received in OAuth callback");
          callbackFuture.completeExceptionally(
              new DatabricksDriverException(
                  "No authorization code received", DatabricksDriverErrorCode.AUTH_ERROR));
        }
      }
    }

    private void sendSuccessResponse(java.io.PrintWriter writer) {
      writer.println("HTTP/1.1 200 OK");
      writer.println("Content-Type: text/html; charset=UTF-8");
      writer.println("Connection: close");
      writer.println();
      writer.println("<!DOCTYPE html>");
      writer.println("<html><head><title>OAuth Login Success</title></head>");
      writer.println("<body><h1>OAuth Login Successful!</h1>");
      writer.println(
          "<p>You have successfully logged in using OAuth. You may now close this tab.</p>");
      writer.println("</body></html>");
      writer.flush();
      LOGGER.debug("Sent success response to OAuth callback");
    }

    private void sendErrorResponse(java.io.PrintWriter writer, String error) {
      writer.println("HTTP/1.1 400 Bad Request");
      writer.println("Content-Type: text/html; charset=UTF-8");
      writer.println("Connection: close");
      writer.println();
      writer.println("<!DOCTYPE html>");
      writer.println("<html><head><title>OAuth Error</title></head>");
      writer.println("<body><h1>OAuth Error</h1>");
      writer.println("<p>" + error + "</p>");
      writer.println("</body></html>");
      writer.flush();
      LOGGER.warn("Sent error response to OAuth callback: {}", error);
    }

    private Map<String, String> parseQueryString(String query) {
      Map<String, String> params = new HashMap<>();
      for (String pair : query.split("&")) {
        String[] keyValue = pair.split("=", 2);
        if (keyValue.length == 2) {
          try {
            String key = java.net.URLDecoder.decode(keyValue[0], StandardCharsets.UTF_8);
            String value = java.net.URLDecoder.decode(keyValue[1], StandardCharsets.UTF_8);
            params.put(key, value);
          } catch (Exception e) {
            LOGGER.warn("Failed to decode query parameter: {}", pair);
          }
        }
      }
      return params;
    }

    public OAuthCallback waitForCallback(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return callbackFuture.get(timeout, unit);
    }

    public void stop() {
      synchronized (serverLock) {
        if (serverSocket != null && !serverSocket.isClosed()) {
          try {
            serverSocket.close();
            LOGGER.debug("OAuth callback server stopped");
          } catch (IOException e) {
            LOGGER.warn("Error closing callback server: {}", e.getMessage());
          }
        }
      }
    }
  }
}
