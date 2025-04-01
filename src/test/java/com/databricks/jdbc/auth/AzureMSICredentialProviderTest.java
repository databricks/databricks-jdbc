package com.databricks.jdbc.auth;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.HeaderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AzureMSICredentialProviderTest {

  @Mock private IDatabricksConnectionContext mockConnectionContext;

  @Mock private IDatabricksHttpClient mockHttpClient;

  @Mock DatabricksConfig config;

  @Mock CloseableHttpResponse mockHttpResponse;

  @Mock HttpEntity mockEntity;

  private static final String TEST_CLIENT_ID = "test-client-id";
  private static final String TEST_RESOURCE_ID = "test-resource-id";
  private final String TEST_ACCESS_TOKEN = "test-access-token";
  private final String TEST_MANAGEMENT_TOKEN = "test-management-token";
  private final String AZURE_DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d";
  private final String AZURE_MANAGEMENT_ENDPOINT = "https://management.core.windows.net/";
  private final String METADATA_SERVICE_URL =
      "http://169.254.169.254/metadata/identity/oauth2/token";

  private AzureMSICredentialProvider setupProvider() {
    when(mockConnectionContext.getNullableClientId()).thenReturn(TEST_CLIENT_ID);
    when(mockConnectionContext.getAzureWorkspaceResourceId()).thenReturn(TEST_RESOURCE_ID);
    return new AzureMSICredentialProvider(mockConnectionContext, mockHttpClient);
  }

  @Test
  public void testCredentialProviderAuthType() {
    when(mockConnectionContext.getNullableClientId()).thenReturn(TEST_CLIENT_ID);
    when(mockConnectionContext.getAzureWorkspaceResourceId()).thenReturn(TEST_RESOURCE_ID);
    when(mockConnectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
    // Cover the constructor too
    AzureMSICredentialProvider provider = new AzureMSICredentialProvider(mockConnectionContext);
    assertEquals("azure-msi", provider.authType());
  }

  @Test
  public void testConfigure() throws DatabricksHttpException, IOException {
    AzureMSICredentialProvider provider = setupProvider();

    // Capture the HttpGet requests to check their parameters
    ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);

    when(mockHttpClient.execute(requestCaptor.capture())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenAnswer(
            invocation -> {
              HttpGet request = requestCaptor.getValue();
              String uri = request.getURI().toString();
              String jsonResponse;
              if (uri.contains(
                  "resource="
                      + AZURE_MANAGEMENT_ENDPOINT.replace(":", "%3A").replace("/", "%2F"))) {
                // This is the management endpoint request
                jsonResponse = createJsonResponse(TEST_MANAGEMENT_TOKEN);
              } else {
                // This is the Databricks scope request
                jsonResponse = createJsonResponse(TEST_ACCESS_TOKEN);
              }
              return new ByteArrayInputStream(jsonResponse.getBytes());
            });

    HeaderFactory headerFactory = provider.configure(config);
    Map<String, String> headers = headerFactory.headers();

    // Verify the headers are correct
    assertEquals("Bearer " + TEST_ACCESS_TOKEN, headers.get(HttpHeaders.AUTHORIZATION));
    assertEquals(TEST_RESOURCE_ID, headers.get("X-Databricks-Azure-Workspace-Resource-Id"));
    assertEquals(TEST_MANAGEMENT_TOKEN, headers.get("X-Databricks-Azure-SP-Management-Token"));

    // Verify both requests were made correctly
    assertEquals(2, requestCaptor.getAllValues().size());
    boolean foundManagementRequest = false;
    boolean foundScopeRequest = false;

    for (HttpGet request : requestCaptor.getAllValues()) {
      String uri = request.getURI().toString();

      // Check common parameters
      assertTrue(uri.startsWith(METADATA_SERVICE_URL));
      assertTrue(uri.contains("api-version=2021-10-01"));
      assertTrue(uri.contains("client_id=" + TEST_CLIENT_ID));
      assertEquals("true", request.getFirstHeader("Metadata").getValue());

      // Check resource-specific parameters
      if (uri.contains(
          "resource=" + AZURE_MANAGEMENT_ENDPOINT.replace(":", "%3A").replace("/", "%2F"))) {
        foundManagementRequest = true;
      } else if (uri.contains("resource=" + AZURE_DATABRICKS_SCOPE)) {
        foundScopeRequest = true;
      }
    }

    assertTrue(foundManagementRequest, "Management endpoint request was not made");
    assertTrue(foundScopeRequest, "Databricks scope request was not made");
  }

  @Test
  public void testConfigureWithoutClientId() throws DatabricksHttpException, IOException {
    when(mockConnectionContext.getNullableClientId()).thenReturn(null);
    when(mockConnectionContext.getAzureWorkspaceResourceId()).thenReturn(TEST_RESOURCE_ID);
    AzureMSICredentialProvider provider =
        new AzureMSICredentialProvider(mockConnectionContext, mockHttpClient);
    ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);

    when(mockHttpClient.execute(requestCaptor.capture())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenAnswer(
            invocation -> {
              HttpGet request = requestCaptor.getValue();
              String uri = request.getURI().toString();

              String jsonResponse;
              if (uri.contains(
                  "resource="
                      + AZURE_MANAGEMENT_ENDPOINT.replace(":", "%3A").replace("/", "%2F"))) {
                jsonResponse = createJsonResponse(TEST_MANAGEMENT_TOKEN);
              } else {
                jsonResponse = createJsonResponse(TEST_ACCESS_TOKEN);
              }

              return new ByteArrayInputStream(jsonResponse.getBytes());
            });

    HeaderFactory headerFactory = provider.configure(config);
    Map<String, String> headers = headerFactory.headers();
    assertEquals("Bearer " + TEST_ACCESS_TOKEN, headers.get(HttpHeaders.AUTHORIZATION));

    // Verify requests were made without client_id parameter
    for (HttpGet request : requestCaptor.getAllValues()) {
      String uri = request.getURI().toString();
      assertFalse(uri.contains("client_id="), "Request should not contain client_id parameter");
    }
  }

  @Test
  public void testExceptionHandling() throws DatabricksHttpException {
    AzureMSICredentialProvider provider = setupProvider();

    // Make the HTTP client throw an exception
    when(mockHttpClient.execute(any(HttpGet.class)))
        .thenThrow(
            new DatabricksHttpException(
                "Connection failed", DatabricksDriverErrorCode.INVALID_STATE));
    HeaderFactory headerFactory = provider.configure(config);
    Exception exception = assertThrows(DatabricksException.class, headerFactory::headers);

    assertTrue(exception.getMessage().contains("Failed to retrieve Azure MSI token"));
    assertTrue(exception.getMessage().contains("Connection failed"));
    assertTrue(exception.getCause() instanceof DatabricksHttpException);
  }

  @Test
  public void testConfigureWithNullResourceId() throws DatabricksHttpException, IOException {
    when(mockConnectionContext.getNullableClientId()).thenReturn(TEST_CLIENT_ID);
    when(mockConnectionContext.getAzureWorkspaceResourceId()).thenReturn(null);
    AzureMSICredentialProvider provider =
        new AzureMSICredentialProvider(mockConnectionContext, mockHttpClient);
    ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);

    when(mockHttpClient.execute(requestCaptor.capture())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenAnswer(
            invocation ->
                new ByteArrayInputStream(createJsonResponse(TEST_ACCESS_TOKEN).getBytes()));

    HeaderFactory headerFactory = provider.configure(config);
    Map<String, String> headers = headerFactory.headers();

    assertEquals("Bearer " + TEST_ACCESS_TOKEN, headers.get(HttpHeaders.AUTHORIZATION));

    // Verify resource-specific headers are NOT present
    assertNull(
        headers.get("X-Databricks-Azure-Workspace-Resource-Id"),
        "Resource ID header should not be present");
    assertNull(
        headers.get("X-Databricks-Azure-SP-Management-Token"),
        "Management token header should not be present");

    // Verify only one request was made (no management token request)
    assertEquals(
        1,
        requestCaptor.getAllValues().size(),
        "Only one request should be made when resource ID is null");

    // Verify the request contains the right parameters
    HttpGet request = requestCaptor.getValue();
    String uri = request.getURI().toString();
    assertTrue(
        uri.contains("resource=" + AZURE_DATABRICKS_SCOPE),
        "Request should be for Databricks scope");
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
