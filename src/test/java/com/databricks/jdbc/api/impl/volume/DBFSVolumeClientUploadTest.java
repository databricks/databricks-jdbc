package com.databricks.jdbc.api.impl.volume;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.databricks.jdbc.api.impl.VolumeOperationStatus;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.model.client.filesystem.CreateUploadUrlResponse;
import com.databricks.jdbc.model.client.filesystem.VolumePutResult;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Tests for DBFSVolumeClient's file upload functionality, with a focus on the parallel upload, rate
 * limiting, and retry mechanisms.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DBFSVolumeClientUploadTest {

  @TempDir private File tempFolder;

  @Mock private IDatabricksConnectionContext connectionContext;

  @Mock private WorkspaceClient workspaceClient;

  @Mock private ApiClient apiClient;

  @Mock private IDatabricksHttpClient httpClient;

  @Mock private DatabricksConfig databricksConfig;

  @Captor private ArgumentCaptor<AsyncRequestProducer> requestProducerCaptor;

  @Captor private ArgumentCaptor<AsyncResponseConsumer<SimpleHttpResponse>> responseConsumerCaptor;

  @Captor private ArgumentCaptor<FutureCallback<SimpleHttpResponse>> callbackCaptor;

  private DBFSVolumeClient volumeClient;

  @BeforeEach
  public void setUp() throws Exception {
    // Essential setup for creating the volumeClient
    lenient().when(workspaceClient.apiClient()).thenReturn(apiClient);

    volumeClient = new DBFSVolumeClient(workspaceClient);

    // Use reflection to set the httpClient
    Field httpClientField = DBFSVolumeClient.class.getDeclaredField("databricksHttpClient");
    httpClientField.setAccessible(true);
    httpClientField.set(volumeClient, httpClient);

    // Use reflection to set the connectionContext
    Field connectionContextField = DBFSVolumeClient.class.getDeclaredField("connectionContext");
    connectionContextField.setAccessible(true);
    connectionContextField.set(volumeClient, connectionContext);
  }

  /** Test successful file uploads. */
  @Test
  public void testPutFiles_Successful() throws Exception {
    // Setup necessary mocks for this test - use lenient() to avoid UnnecessaryStubbingException
    lenient().when(workspaceClient.config()).thenReturn(databricksConfig);
    Map<String, String> authHeaders = new HashMap<>();
    authHeaders.put("Authorization", "Bearer test-token");
    lenient().when(databricksConfig.authenticate()).thenReturn(authHeaders);
    lenient().when(apiClient.serialize(any())).thenReturn("{}");

    // Prepare test files
    File file1 = createTestFile("file1.txt", "test content 1");
    File file2 = createTestFile("file2.txt", "test content 2");

    List<String> objectPaths = Arrays.asList("path/to/file1.txt", "path/to/file2.txt");
    List<String> localPaths = Arrays.asList(file1.getAbsolutePath(), file2.getAbsolutePath());

    // Setup test with mock URL request response
    setupMockPresignedUrlFuture("https://presigned-url.example.com");

    // Mock HTTP client to execute async requests successfully
    doAnswer(
            invocation -> {
              FutureCallback<SimpleHttpResponse> callback = invocation.getArgument(2);

              // Create successful response
              SimpleHttpResponse response = mock(SimpleHttpResponse.class);
              lenient().when(response.getCode()).thenReturn(200);
              lenient().when(response.getReasonPhrase()).thenReturn("OK");

              // Call the callback with successful response
              callback.completed(response);
              return null;
            })
        .when(httpClient)
        .executeAsync(any(), any(), any());

    // Execute putFiles
    List<VolumePutResult> results =
        volumeClient.putFiles("catalog", "schema", "volume", objectPaths, localPaths, false);

    // Verify results
    assertEquals(2, results.size(), "Should have 2 results");
    for (VolumePutResult result : results) {
      assertEquals(
          VolumeOperationStatus.SUCCEEDED, result.getStatus(), "Status should be SUCCEEDED");
      assertEquals(200, result.getStatusCode(), "Status code should be 200");
    }

    // Verify HTTP client was called twice (once for each file)
    verify(httpClient, times(2)).executeAsync(any(), any(), any());
  }

  /** Test successful input stream uploads. */
  @Test
  public void testPutInputStream_Successful() throws Exception {
    // Setup necessary mocks for this test
    lenient().when(workspaceClient.config()).thenReturn(databricksConfig);
    Map<String, String> authHeaders = new HashMap<>();
    authHeaders.put("Authorization", "Bearer test-token");
    lenient().when(databricksConfig.authenticate()).thenReturn(authHeaders);
    lenient().when(apiClient.serialize(any())).thenReturn("{}");

    // Prepare test input streams
    byte[] content1 = "test content 1".getBytes();
    byte[] content2 = "test content 2".getBytes();
    List<InputStream> streams =
        Arrays.asList(new ByteArrayInputStream(content1), new ByteArrayInputStream(content2));
    List<String> objectPaths = Arrays.asList("path/to/file1.txt", "path/to/file2.txt");
    List<Long> contentLengths = Arrays.asList((long) content1.length, (long) content2.length);

    // Setup test with mock URL request response
    setupMockPresignedUrlFuture("https://presigned-url.example.com");

    // Mock HTTP client to execute async requests successfully
    doAnswer(
            invocation -> {
              FutureCallback<SimpleHttpResponse> callback = invocation.getArgument(2);

              // Create successful response
              SimpleHttpResponse response = mock(SimpleHttpResponse.class);
              lenient().when(response.getCode()).thenReturn(200);
              lenient().when(response.getReasonPhrase()).thenReturn("OK");

              // Call the callback with successful response
              callback.completed(response);
              return null;
            })
        .when(httpClient)
        .executeAsync(any(), any(), any());

    // Execute putFiles with input streams
    List<VolumePutResult> results =
        volumeClient.putFiles(
            "catalog", "schema", "volume", objectPaths, streams, contentLengths, false);

    // Verify results
    assertEquals(2, results.size(), "Should have 2 results");
    for (VolumePutResult result : results) {
      assertEquals(
          VolumeOperationStatus.SUCCEEDED, result.getStatus(), "Status should be SUCCEEDED");
      assertEquals(200, result.getStatusCode(), "Status code should be 200");
    }

    // Verify HTTP client was called twice (once for each stream)
    verify(httpClient, times(2)).executeAsync(any(), any(), any());
  }

  /** Test retry functionality when getting a server error. */
  @Test
  public void testPutFiles_ServerError_WithRetry() throws Exception {
    // Setup necessary mocks for this test
    lenient().when(workspaceClient.config()).thenReturn(databricksConfig);
    Map<String, String> authHeaders = new HashMap<>();
    authHeaders.put("Authorization", "Bearer test-token");
    lenient().when(databricksConfig.authenticate()).thenReturn(authHeaders);
    lenient().when(apiClient.serialize(any())).thenReturn("{}");

    // Setup connectionContext mock for retry logic
    lenient()
        .when(connectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(java.util.Arrays.asList(408, 429, 500, 502, 503, 504));
    lenient().when(connectionContext.getUCIngestionRetryTimeoutSeconds()).thenReturn(900);

    // Prepare test file
    File file = createTestFile("retry-test.txt", "test content");
    List<String> objectPaths = Arrays.asList("path/to/retry-test.txt");
    List<String> localPaths = Arrays.asList(file.getAbsolutePath());

    // Setup test with mock URL request response
    setupMockPresignedUrlFuture("https://presigned-url.example.com");

    // Create a counter to track retry attempts
    final int[] attemptCounter = {0};

    // Mock HTTP client to fail with 500 on first attempt, then succeed on retry
    doAnswer(
            invocation -> {
              FutureCallback<SimpleHttpResponse> callback = invocation.getArgument(2);

              attemptCounter[0]++;
              if (attemptCounter[0] == 1) {
                // First attempt - return 500 error
                SimpleHttpResponse errorResponse = mock(SimpleHttpResponse.class);
                lenient().when(errorResponse.getCode()).thenReturn(500);
                lenient().when(errorResponse.getReasonPhrase()).thenReturn("Internal Server Error");
                callback.completed(errorResponse);
              } else {
                // Second attempt - return success
                SimpleHttpResponse successResponse = mock(SimpleHttpResponse.class);
                lenient().when(successResponse.getCode()).thenReturn(200);
                lenient().when(successResponse.getReasonPhrase()).thenReturn("OK");
                callback.completed(successResponse);
              }
              return null;
            })
        .when(httpClient)
        .executeAsync(any(), any(), any());

    // Execute putFiles
    List<VolumePutResult> results =
        volumeClient.putFiles("catalog", "schema", "volume", objectPaths, localPaths, false);

    // Verify results
    assertEquals(1, results.size(), "Should have 1 result");
    assertEquals(
        VolumeOperationStatus.SUCCEEDED, results.get(0).getStatus(), "Status should be SUCCEEDED");
    assertEquals(200, results.get(0).getStatusCode(), "Status code should be 200");

    // Verify HTTP client was called twice (initial + retry)
    verify(httpClient, times(2)).executeAsync(any(), any(), any());
  }

  /** Test handling of invalid file paths. */
  @Test
  public void testPutFiles_InvalidFileSkipped() throws Exception {
    // Setup necessary mocks for this test
    lenient().when(workspaceClient.config()).thenReturn(databricksConfig);
    Map<String, String> authHeaders = new HashMap<>();
    authHeaders.put("Authorization", "Bearer test-token");
    lenient().when(databricksConfig.authenticate()).thenReturn(authHeaders);
    lenient().when(apiClient.serialize(any())).thenReturn("{}");
    lenient().when(connectionContext.getHostUrl()).thenReturn("https://test.databricks.com");
    lenient().when(connectionContext.getVolumeOperationAllowedPaths()).thenReturn("");

    // Prepare one valid and one invalid file
    File validFile = createTestFile("valid.txt", "valid content");
    String invalidPath = "/nonexistent/path/to/file.txt";

    // Create lists for valid and invalid files - order is important!
    // First file is valid, second file is invalid
    List<String> objectPaths = Arrays.asList("path/to/valid.txt", "path/to/invalid.txt");
    List<String> localPaths = Arrays.asList(validFile.getAbsolutePath(), invalidPath);

    // Setup test with mock URL request response
    setupMockPresignedUrlFuture("https://presigned-url.example.com");

    // Mock HTTP client to execute async requests successfully
    doAnswer(
            invocation -> {
              FutureCallback<SimpleHttpResponse> callback = invocation.getArgument(2);
              SimpleHttpResponse response = mock(SimpleHttpResponse.class);
              lenient().when(response.getCode()).thenReturn(200);
              lenient().when(response.getReasonPhrase()).thenReturn("OK");
              callback.completed(response);
              return null;
            })
        .when(httpClient)
        .executeAsync(any(), any(), any());

    // Execute putFiles
    List<VolumePutResult> results =
        volumeClient.putFiles("catalog", "schema", "volume", objectPaths, localPaths, false);

    // Debug info
    System.out.println("=== Test Results ===");
    for (int i = 0; i < results.size(); i++) {
      System.out.println(
          "Result "
              + i
              + " - Status: "
              + results.get(i).getStatus()
              + ", Code: "
              + results.get(i).getStatusCode()
              + ", Message: "
              + results.get(i).getMessage());
    }

    // Verify results size
    assertEquals(2, results.size(), "Should have 2 results");

    // The results should now be in the same order as input files
    // First result (index 0) should be for the valid file
    assertEquals(
        VolumeOperationStatus.SUCCEEDED,
        results.get(0).getStatus(),
        "First result (valid file) status should be SUCCEEDED");
    assertEquals(
        200, results.get(0).getStatusCode(), "Valid file result status code should be 200");

    // Second result (index 1) should be for the invalid file
    assertEquals(
        VolumeOperationStatus.FAILED,
        results.get(1).getStatus(),
        "Second result (invalid file) status should be FAILED");
    assertEquals(
        400, results.get(1).getStatusCode(), "Invalid file result status code should be 404");
    assertTrue(
        results.get(1).getMessage().contains("File not found"),
        "Invalid file result should have the appropriate error message");

    // Verify HTTP client was only once (for the valid file)
    verify(httpClient, times(1)).executeAsync(any(), any(), any());
  }

  /** Test handling of presigned URL request failures. */
  @Test
  public void testPutFiles_PresignedUrlFailure() throws Exception {
    // Setup necessary mocks for this test
    lenient().when(workspaceClient.config()).thenReturn(databricksConfig);
    Map<String, String> authHeaders = new HashMap<>();
    authHeaders.put("Authorization", "Bearer test-token");
    lenient().when(databricksConfig.authenticate()).thenReturn(authHeaders);
    lenient().when(apiClient.serialize(any())).thenReturn("{}");

    // Prepare test file
    File file = createTestFile("url-error.txt", "test content");
    List<String> objectPaths = Arrays.asList("path/to/url-error.txt");
    List<String> localPaths = Arrays.asList(file.getAbsolutePath());

    // Setup test with failed presigned URL future
    CompletableFuture<CreateUploadUrlResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new IOException("Failed to get presigned URL"));

    // Use reflection to set up the mock for requestPresignedUrlWithRetry
    Method method =
        volumeClient
            .getClass()
            .getDeclaredMethod(
                "requestPresignedUrlWithRetry", String.class, String.class, int.class);
    method.setAccessible(true);

    // Create a spy of the client to mock the method
    DBFSVolumeClient spyClient = spy(volumeClient);
    doReturn(failedFuture)
        .when(spyClient)
        .requestPresignedUrlWithRetry(anyString(), anyString(), anyInt());

    // Execute putFiles
    List<VolumePutResult> results =
        spyClient.putFiles("catalog", "schema", "volume", objectPaths, localPaths, false);

    // Verify results
    assertEquals(1, results.size(), "Should have 1 result");
    assertEquals(
        VolumeOperationStatus.FAILED, results.get(0).getStatus(), "Status should be FAILED");
    assertEquals(500, results.get(0).getStatusCode(), "Status code should be 500");

    // Verify HTTP client was not called (since presigned URL failed)
    verify(httpClient, never()).executeAsync(any(), any(), any());
  }

  /** Test handling of upload exceptions. */
  @Test
  public void testPutFiles_UploadException() throws Exception {
    // Setup necessary mocks for this test
    lenient().when(workspaceClient.config()).thenReturn(databricksConfig);
    Map<String, String> authHeaders = new HashMap<>();
    authHeaders.put("Authorization", "Bearer test-token");
    lenient().when(databricksConfig.authenticate()).thenReturn(authHeaders);
    lenient().when(apiClient.serialize(any())).thenReturn("{}");

    // Setup connectionContext mock for retry logic
    lenient()
        .when(connectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(java.util.Arrays.asList(408, 429, 500, 502, 503, 504));
    lenient().when(connectionContext.getUCIngestionRetryTimeoutSeconds()).thenReturn(900);

    // Prepare test file
    File file = createTestFile("exception.txt", "test content");
    List<String> objectPaths = Arrays.asList("path/to/exception.txt");
    List<String> localPaths = Arrays.asList(file.getAbsolutePath());

    // Setup test with mock URL request response
    setupMockPresignedUrlFuture("https://presigned-url.example.com");

    // Mock HTTP client to throw exception during execution
    doAnswer(
            invocation -> {
              FutureCallback<SimpleHttpResponse> callback = invocation.getArgument(2);
              callback.failed(new IOException("Network error during upload"));
              return null;
            })
        .when(httpClient)
        .executeAsync(any(), any(), any());

    // Execute putFiles
    List<VolumePutResult> results =
        volumeClient.putFiles("catalog", "schema", "volume", objectPaths, localPaths, false);

    // Verify results
    assertEquals(1, results.size(), "Should have 1 result");
    assertEquals(
        VolumeOperationStatus.FAILED, results.get(0).getStatus(), "Status should be FAILED");
    assertEquals(500, results.get(0).getStatusCode(), "Status code should be 500");
    assertTrue(
        results.get(0).getMessage().contains("Network error during upload"),
        "Error message should contain the exception");

    // Verify HTTP client was called three times (initial attempt + 2 retries = 3 total attempts)
    // This matches the VolumeUploadCallback.MAX_UPLOAD_RETRIES=3 setting
    verify(httpClient, times(3)).executeAsync(any(), any(), any());
  }

  /** Test rate limiting functionality with the semaphore. */
  @Test
  public void testRateLimiting() throws Exception {
    // Setup necessary mocks for this test
    lenient().when(workspaceClient.config()).thenReturn(databricksConfig);
    Map<String, String> authHeaders = new HashMap<>();
    authHeaders.put("Authorization", "Bearer test-token");
    lenient().when(databricksConfig.authenticate()).thenReturn(authHeaders);
    lenient().when(apiClient.serialize(any())).thenReturn("{}");
    lenient().when(connectionContext.getHostUrl()).thenReturn("https://test.databricks.com");

    // Prepare test files - many to trigger rate limiting
    int numFiles = 30; // Enough to test rate limiting
    List<String> objectPaths = new ArrayList<>();
    List<String> localPaths = new ArrayList<>();

    for (int i = 0; i < numFiles; i++) {
      File file = createTestFile("file" + i + ".txt", "content " + i);
      objectPaths.add("path/to/file" + i + ".txt");
      localPaths.add(file.getAbsolutePath());
    }

    // Setup test with mock URL request response
    CreateUploadUrlResponse urlResponse = new CreateUploadUrlResponse();
    urlResponse.setUrl("https://presigned-url.example.com");

    // Count how many concurrent requests are made
    final int[] maxConcurrent = {0};
    final int[] currentConcurrent = {0};

    // Create a semaphore with very limited permits to clearly test rate limiting
    Semaphore testSemaphore = new Semaphore(5);

    // Use reflection to set the semaphore
    Field semaphoreField = DBFSVolumeClient.class.getDeclaredField("presignedUrlSemaphore");
    semaphoreField.setAccessible(true);
    semaphoreField.set(volumeClient, testSemaphore);

    // Mock HTTP client to simulate concurrent work
    doAnswer(
            invocation -> {
              // Acquire semaphore to simulate rate limiting
              testSemaphore.acquire();

              // Track concurrent requests
              synchronized (maxConcurrent) {
                currentConcurrent[0]++;
                maxConcurrent[0] = Math.max(maxConcurrent[0], currentConcurrent[0]);
              }

              // Complete the future with a presigned URL
              CompletableFuture<CreateUploadUrlResponse> future = new CompletableFuture<>();
              future.complete(urlResponse);

              // Simulate network delay
              Thread.sleep(20);

              // Simulate upload success
              FutureCallback<SimpleHttpResponse> callback = invocation.getArgument(2);
              SimpleHttpResponse response = mock(SimpleHttpResponse.class);
              lenient().when(response.getCode()).thenReturn(200);
              callback.completed(response);

              // Release semaphore
              testSemaphore.release();

              // Decrease counter
              synchronized (maxConcurrent) {
                currentConcurrent[0]--;
              }

              return null;
            })
        .when(httpClient)
        .executeAsync(any(), any(), any());

    // Use reflection to create a spy and mock requestPresignedUrlWithRetry
    Method method =
        volumeClient
            .getClass()
            .getDeclaredMethod(
                "requestPresignedUrlWithRetry", String.class, String.class, int.class);
    method.setAccessible(true);
    DBFSVolumeClient spyClient = spy(volumeClient);

    // Prepare the CompletableFuture that the mock will return
    CompletableFuture<CreateUploadUrlResponse> mockFuture =
        CompletableFuture.completedFuture(urlResponse);
    doReturn(mockFuture)
        .when(spyClient)
        .requestPresignedUrlWithRetry(anyString(), anyString(), anyInt());

    // Execute putFiles
    List<VolumePutResult> results =
        spyClient.putFiles("catalog", "schema", "volume", objectPaths, localPaths, false);

    // Verify results
    assertEquals(numFiles, results.size(), "Should have results for all files");

    // Verify max concurrency was limited
    assertTrue(maxConcurrent[0] <= 5, "Max concurrency should be limited");
  }

  // Helper method to setup mock for presigned URL futures
  private void setupMockPresignedUrlFuture(String url) throws Exception {
    // Create presigned URL response
    CreateUploadUrlResponse urlResponse = new CreateUploadUrlResponse();
    urlResponse.setUrl(url);

    // Create completed future
    CompletableFuture<CreateUploadUrlResponse> presignedUrlFuture =
        CompletableFuture.completedFuture(urlResponse);

    // Use reflection to access the private method
    Method method =
        volumeClient
            .getClass()
            .getDeclaredMethod(
                "requestPresignedUrlWithRetry", String.class, String.class, int.class);
    method.setAccessible(true);

    // Create a spy to mock the method
    DBFSVolumeClient spyClient = spy(volumeClient);
    doReturn(presignedUrlFuture)
        .when(spyClient)
        .requestPresignedUrlWithRetry(anyString(), anyString(), anyInt());

    // Replace the original client with the spy
    volumeClient = spyClient;
  }

  // Helper method to create a test file
  private File createTestFile(String filename, String content) throws IOException {
    File file = new File(tempFolder, filename);
    java.nio.file.Files.write(file.toPath(), content.getBytes());
    return file;
  }

  /** Test executeUploads method with mixed valid and invalid requests. */
  @Test
  public void testExecuteUploads_MixedValidInvalid() throws Exception {
    // Setup necessary mocks
    lenient().when(workspaceClient.config()).thenReturn(databricksConfig);
    Map<String, String> authHeaders = new HashMap<>();
    authHeaders.put("Authorization", "Bearer test-token");
    lenient().when(databricksConfig.authenticate()).thenReturn(authHeaders);
    lenient().when(apiClient.serialize(any())).thenReturn("{}");

    // Create one valid file and one invalid path
    File validFile = createTestFile("valid.txt", "valid content");
    String invalidPath = "/non/existent/path/invalid.txt";

    List<String> objectPaths = Arrays.asList("valid.txt", "invalid.txt");
    List<String> localPaths = Arrays.asList(validFile.getAbsolutePath(), invalidPath);

    // Setup mock for presigned URL
    setupMockPresignedUrlFuture("https://presigned-url.example.com");

    // Mock HTTP client to succeed for valid uploads
    doAnswer(
            invocation -> {
              FutureCallback<SimpleHttpResponse> callback = invocation.getArgument(2);
              SimpleHttpResponse response = mock(SimpleHttpResponse.class);
              lenient().when(response.getCode()).thenReturn(200);
              lenient().when(response.getReasonPhrase()).thenReturn("OK");
              callback.completed(response);
              return null;
            })
        .when(httpClient)
        .executeAsync(any(), any(), any());

    // Execute putFiles
    List<VolumePutResult> results =
        volumeClient.putFiles("catalog", "schema", "volume", objectPaths, localPaths, false);

    // Verify results
    assertEquals(2, results.size());

    // First result should be successful
    assertEquals(VolumeOperationStatus.SUCCEEDED, results.get(0).getStatus());
    assertEquals(200, results.get(0).getStatusCode());

    // Second result should be failed (file not found)
    assertEquals(VolumeOperationStatus.FAILED, results.get(1).getStatus());
    assertEquals(400, results.get(1).getStatusCode());
    assertTrue(results.get(1).getMessage().contains("File not found"));

    // Verify HTTP client was only once (for the valid file)
    verify(httpClient, times(1)).executeAsync(any(), any(), any());
  }

  /** Test putFiles with mismatched array sizes. */
  @Test
  public void testPutFiles_MismatchedArraySizes() {
    List<String> objectPaths = Arrays.asList("file1.txt", "file2.txt");
    List<String> localPaths = Arrays.asList("path1.txt"); // Only one path

    // Should throw IllegalArgumentException
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                volumeClient.putFiles(
                    "catalog", "schema", "volume", objectPaths, localPaths, false));

    assertTrue(exception.getMessage().contains("objectPaths and localPaths – sizes differ"));
  }

  /** Test putFiles with input streams - mismatched array sizes. */
  @Test
  public void testPutFiles_InputStreams_MismatchedArraySizes() {
    List<String> objectPaths = Arrays.asList("file1.txt", "file2.txt");
    List<InputStream> inputStreams = Arrays.asList(new ByteArrayInputStream("test".getBytes()));
    List<Long> contentLengths = Arrays.asList(4L, 5L);

    // Should throw IllegalArgumentException
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                volumeClient.putFiles(
                    "catalog",
                    "schema",
                    "volume",
                    objectPaths,
                    inputStreams,
                    contentLengths,
                    false));

    assertTrue(
        exception
            .getMessage()
            .contains("objectPaths, inputStreams, contentLengths – sizes differ"));
  }

  /** Test putFiles with empty lists. */
  @Test
  public void testPutFiles_EmptyLists() throws Exception {
    List<String> objectPaths = Arrays.asList();
    List<String> localPaths = Arrays.asList();

    // Execute putFiles
    List<VolumePutResult> results =
        volumeClient.putFiles("catalog", "schema", "volume", objectPaths, localPaths, false);

    // Verify results
    assertEquals(0, results.size());

    // Verify HTTP client was never called
    verify(httpClient, never()).executeAsync(any(), any(), any());
  }
}
