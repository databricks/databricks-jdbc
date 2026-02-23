package com.databricks.jdbc.auth;

import static com.databricks.jdbc.TestConstants.TEST_ACCESS_TOKEN;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.oauth.Token;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AzureMSICredentialsTest {

  @Mock private IDatabricksHttpClient mockHttpClient;
  @Mock private CloseableHttpResponse mockHttpResponse;
  @Mock private HttpEntity mockEntity;

  private static final String AZURE_TEST_CLIENT_ID = "test-client-id-123";
  private static final String AZURE_DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d";
  private static final String AZURE_MANAGEMENT_ENDPOINT = "https://management.core.windows.net/";
  private static final String METADATA_SERVICE_URL =
      "http://169.254.169.254/metadata/identity/oauth2/token";

  @BeforeEach
  void setUp() {
    reset(mockHttpClient, mockHttpResponse, mockEntity);
  }

  private void givenSuccessfulTokenResponse(String accessToken) throws Exception {
    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
    when(mockHttpResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(new ByteArrayInputStream(createJsonResponse(accessToken).getBytes()));
  }

  @Test
  void should_GetToken_When_SystemAssignedIdentity() throws Exception {
    givenSuccessfulTokenResponse(TEST_ACCESS_TOKEN);

    // Create credentials without client ID (system-assigned)
    AzureMSICredentials credentials = new AzureMSICredentials(mockHttpClient, null);

    // Get token
    Token token = credentials.getToken();

    // Verify token is returned
    assertNotNull(token);
    assertEquals(TEST_ACCESS_TOKEN, token.getAccessToken());
    assertEquals("Bearer", token.getTokenType());
    assertTrue(token.getExpiry().isAfter(Instant.now()));

    // Verify request was made correctly
    ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);
    verify(mockHttpClient).execute(requestCaptor.capture());

    HttpGet request = requestCaptor.getValue();
    String uri = request.getURI().toString();

    assertTrue(uri.startsWith(METADATA_SERVICE_URL));
    assertTrue(uri.contains("api-version=2021-10-01"));
    assertTrue(uri.contains("resource=" + AZURE_DATABRICKS_SCOPE));
    assertFalse(uri.contains("client_id"));
    assertEquals("true", request.getFirstHeader("Metadata").getValue());
  }

  @Test
  void should_GetToken_When_UserAssignedIdentity() throws Exception {
    givenSuccessfulTokenResponse(TEST_ACCESS_TOKEN);
    // Create credentials with client ID (user-assigned)
    AzureMSICredentials credentials = new AzureMSICredentials(mockHttpClient, AZURE_TEST_CLIENT_ID);

    // Get token
    Token token = credentials.getToken();

    // Verify token is returned
    assertNotNull(token);
    assertEquals(TEST_ACCESS_TOKEN, token.getAccessToken());

    // Verify request includes client_id
    ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);
    verify(mockHttpClient).execute(requestCaptor.capture());

    HttpGet request = requestCaptor.getValue();
    String uri = request.getURI().toString();

    assertTrue(uri.contains("client_id=" + AZURE_TEST_CLIENT_ID));
  }

  @Test
  void should_GetManagementEndpointToken_Successfully() throws Exception {
    givenSuccessfulTokenResponse(TEST_ACCESS_TOKEN);
    AzureMSICredentials credentials = new AzureMSICredentials(mockHttpClient, null);

    // Get management endpoint token
    Token token = credentials.getManagementEndpointToken();

    // Verify token is returned
    assertNotNull(token);
    assertEquals(TEST_ACCESS_TOKEN, token.getAccessToken());

    // Verify request was for management endpoint
    ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);
    verify(mockHttpClient, atLeastOnce()).execute(requestCaptor.capture());

    boolean foundManagementRequest =
        requestCaptor.getAllValues().stream()
            .anyMatch(
                req -> {
                  String uri = req.getURI().toString();
                  return uri.contains(
                      "resource="
                          + AZURE_MANAGEMENT_ENDPOINT.replace(":", "%3A").replace("/", "%2F"));
                });

    assertTrue(foundManagementRequest);
  }

  @Test
  void should_CacheToken_When_MultipleCallsMade() throws Exception {
    givenSuccessfulTokenResponse(TEST_ACCESS_TOKEN);
    AzureMSICredentials credentials = new AzureMSICredentials(mockHttpClient, null);

    // Get token multiple times
    Token token1 = credentials.getToken();
    Token token2 = credentials.getToken();
    Token token3 = credentials.getToken();

    // All tokens should be the same (cached)
    assertNotNull(token1);
    assertSame(token1, token2);
    assertSame(token1, token3);

    // Verify only one HTTP call was made (token was cached)
    verify(mockHttpClient, times(1)).execute(any(HttpGet.class));
  }

  @Test
  void should_CacheManagementTokenSeparately() throws Exception {
    // Setup mock response to return different tokens
    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
    when(mockHttpResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(
            new ByteArrayInputStream(createJsonResponse("token1").getBytes()),
            new ByteArrayInputStream(createJsonResponse("token2").getBytes()));

    AzureMSICredentials credentials = new AzureMSICredentials(mockHttpClient, null);

    // Get both types of tokens
    Token databricksToken = credentials.getToken();
    Token managementToken = credentials.getManagementEndpointToken();

    // Verify two separate HTTP calls were made
    verify(mockHttpClient, times(2)).execute(any(HttpGet.class));

    // Get tokens again
    Token databricksToken2 = credentials.getToken();
    Token managementToken2 = credentials.getManagementEndpointToken();

    // Verify tokens are cached (no additional HTTP calls)
    verify(mockHttpClient, times(2)).execute(any(HttpGet.class));
    assertSame(databricksToken, databricksToken2);
    assertSame(managementToken, managementToken2);
  }

  @Test
  void should_ThrowException_When_HttpClientThrowsException() throws Exception {
    // Setup mock to throw exception
    when(mockHttpClient.execute(any(HttpGet.class)))
        .thenThrow(
            new DatabricksHttpException("Network error", DatabricksDriverErrorCode.INVALID_STATE));

    AzureMSICredentials credentials = new AzureMSICredentials(mockHttpClient, null);

    // Verify exception is thrown
    DatabricksException exception = assertThrows(DatabricksException.class, credentials::getToken);

    assertTrue(exception.getMessage().contains("Failed to retrieve Azure MSI token"));
    assertTrue(exception.getMessage().contains("Network error"));
    assertTrue(DatabricksHttpException.class.isInstance(exception.getCause()));
  }

  @Test
  void should_ThrowException_When_ResponseParsingFails() throws Exception {
    // Setup mock to return invalid JSON
    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
    when(mockHttpResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream("invalid json".getBytes()));

    AzureMSICredentials credentials = new AzureMSICredentials(mockHttpClient, null);

    // Verify exception is thrown
    DatabricksException exception = assertThrows(DatabricksException.class, credentials::getToken);

    assertTrue(exception.getMessage().contains("Failed to retrieve Azure MSI token"));
  }

  @Test
  void should_ThrowException_When_IOExceptionOccurs() throws Exception {
    // Setup mock to throw IOException
    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
    when(mockHttpResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent()).thenThrow(new IOException("IO error"));

    AzureMSICredentials credentials = new AzureMSICredentials(mockHttpClient, null);

    // Verify exception is thrown
    DatabricksException exception = assertThrows(DatabricksException.class, credentials::getToken);

    assertTrue(exception.getMessage().contains("Failed to retrieve Azure MSI token"));
    assertTrue(IOException.class.isInstance(exception.getCause()));
  }

  @ParameterizedTest
  @CsvSource({"null", "''", "'test-client-id'"})
  void should_HandleVariousClientIdValues(String clientIdInput) throws Exception {
    String clientId = "null".equals(clientIdInput) ? null : clientIdInput.replace("'", "");
    givenSuccessfulTokenResponse(TEST_ACCESS_TOKEN);
    AzureMSICredentials credentials = new AzureMSICredentials(mockHttpClient, clientId);

    // Get token
    Token token = credentials.getToken();

    // Verify token is returned
    assertNotNull(token);

    // Verify request includes or excludes client_id appropriately
    ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);
    verify(mockHttpClient).execute(requestCaptor.capture());

    String uri = requestCaptor.getValue().getURI().toString();
    if (clientId != null && !clientId.isEmpty()) {
      assertTrue(uri.contains("client_id=" + clientId));
    } else if (clientId == null) {
      assertFalse(uri.contains("client_id"));
    } else {
      // Empty string case - client_id will be present but empty
      assertTrue(uri.contains("client_id="));
    }
  }

  @Test
  void should_IncludeRefreshToken_When_PresentInResponse() throws Exception {
    String refreshToken = "test-refresh-token";
    String jsonWithRefresh =
        "{"
            + "\"access_token\": \""
            + TEST_ACCESS_TOKEN
            + "\","
            + "\"expires_in\": 3600,"
            + "\"token_type\": \"Bearer\","
            + "\"refresh_token\": \""
            + refreshToken
            + "\""
            + "}";
    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
    when(mockHttpResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(jsonWithRefresh.getBytes()));

    AzureMSICredentials credentials = new AzureMSICredentials(mockHttpClient, null);

    Token token = credentials.getToken();

    assertNotNull(token);
    assertEquals(refreshToken, token.getRefreshToken());
  }

  @Test
  void should_SetCorrectExpiry_BasedOnExpiresIn() throws Exception {
    int expiresInSeconds = 7200; // 2 hours
    String jsonResponse =
        "{"
            + "\"access_token\": \""
            + TEST_ACCESS_TOKEN
            + "\","
            + "\"expires_in\": "
            + expiresInSeconds
            + ","
            + "\"token_type\": \"Bearer\""
            + "}";
    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockHttpResponse);
    when(mockHttpResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(jsonResponse.getBytes()));

    AzureMSICredentials credentials = new AzureMSICredentials(mockHttpClient, null);

    Instant beforeCall = Instant.now();
    Token token = credentials.getToken();
    Instant afterCall = Instant.now();

    assertNotNull(token);
    assertTrue(
        token.getExpiry().isAfter(beforeCall.plusSeconds(expiresInSeconds - 1))
            && token.getExpiry().isBefore(afterCall.plusSeconds(expiresInSeconds + 1)));
  }

  private String createJsonResponse(String accessToken) {
    return "{"
        + "\"access_token\": \""
        + accessToken
        + "\","
        + "\"expires_in\": 3600,"
        + "\"token_type\": \"Bearer\""
        + "}";
  }
}
