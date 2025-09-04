package com.databricks.jdbc.common.safe;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DatabricksDriverFeatureFlagsContextTest {

  @Mock private IDatabricksConnectionContext connectionContextMock;
  @Mock private IDatabricksHttpClient httpClientMock;
  @Mock private CloseableHttpResponse httpResponseMock;
  @Mock private StatusLine statusLineMock;
  @Mock private HttpEntity httpEntityMock;
  @Mock private ObjectMapper objectMapperMock;
  private static final String FEATURE_FLAG_NAME = "featureFlagName";
  private static final String FEATURE_FLAGS_ENDPOINT =
      "https://test-host/api/2.0/connector-service/feature-flags/OSS_JDBC/1.0.9";

  private DatabricksDriverFeatureFlagsContext context;

  @BeforeEach
  void setUp() {
    // Mock the host for OAuth to return a test host
    when(connectionContextMock.getHostForOAuth()).thenReturn("test-host");
    context = new DatabricksDriverFeatureFlagsContext(connectionContextMock, new HashMap<>());
  }

  private String createFeatureFlagsJson(String flagName, String flagValue, int ttlSeconds)
      throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    java.util.Map<String, Object> flag = new java.util.HashMap<String, Object>();
    flag.put("name", flagName);
    flag.put("value", flagValue);
    java.util.Map<String, Object> response = new java.util.HashMap<String, Object>();
    response.put("flags", java.util.Collections.singletonList(flag));
    response.put("ttlSeconds", ttlSeconds);
    return mapper.writeValueAsString(response);
  }

  private FeatureFlagsResponse createFeatureFlagsResponseWithNoFlags(Integer ttl) throws Exception {
    FeatureFlagsResponse response = new FeatureFlagsResponse();
    Field flagsField = FeatureFlagsResponse.class.getDeclaredField("flags");
    flagsField.setAccessible(true);
    flagsField.set(response, new ArrayList<>());
    Field ttlField = FeatureFlagsResponse.class.getDeclaredField("ttlSeconds");
    ttlField.setAccessible(true);
    ttlField.set(response, ttl);
    return response;
  }

  private FeatureFlagsResponse createFeatureFlagsResponseNullFlags(Integer ttl) throws Exception {
    FeatureFlagsResponse response = new FeatureFlagsResponse();
    Field ttlField = FeatureFlagsResponse.class.getDeclaredField("ttlSeconds");
    ttlField.setAccessible(true);
    ttlField.set(response, ttl);
    return response;
  }

  private FeatureFlagsResponse createFeatureFlagsResponse(
      String flagName, String flagValue, int ttlSeconds) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    java.util.Map<String, Object> flag = new java.util.HashMap<String, Object>();
    flag.put("name", flagName);
    flag.put("value", flagValue);
    java.util.Map<String, Object> response = new java.util.HashMap<String, Object>();
    response.put("flags", java.util.Collections.singletonList(flag));
    response.put("ttlSeconds", ttlSeconds);
    return mapper.readValue(mapper.writeValueAsString(response), FeatureFlagsResponse.class);
  }

  @Test
  void testFetchAndSetFlagsFromServer_Success() throws Exception {
    try (MockedStatic<JsonUtil> jsonUtilMocked = mockStatic(JsonUtil.class)) {
      String responseJson =
          "{\"flags\":[{\"name\":\"test_feature\",\"value\":\"true\"}],\"ttl_seconds\":300}";
      when(httpResponseMock.getStatusLine()).thenReturn(statusLineMock);
      when(statusLineMock.getStatusCode()).thenReturn(200);
      when(httpResponseMock.getEntity()).thenReturn(httpEntityMock);
      when(httpEntityMock.getContent())
          .thenReturn(new ByteArrayInputStream(responseJson.getBytes()));
      when(httpClientMock.execute(any(HttpGet.class))).thenReturn(httpResponseMock);
      FeatureFlagsResponse response =
          new ObjectMapper()
              .readValue(
                  createFeatureFlagsJson(FEATURE_FLAG_NAME, "true", 300),
                  FeatureFlagsResponse.class);
      jsonUtilMocked.when(JsonUtil::getMapper).thenReturn(objectMapperMock);
      when(objectMapperMock.readValue(anyString(), eq(FeatureFlagsResponse.class)))
          .thenReturn(response);
      HttpGet request = new HttpGet(FEATURE_FLAGS_ENDPOINT);
      context.fetchAndSetFlagsFromServer(httpClientMock, request);
      assertTrue(context.isFeatureEnabled(FEATURE_FLAG_NAME));
      verify(httpClientMock).execute(request);
    }
  }

  @Test
  void testFetchAndSetFlagsFromServer_WithCustomTTL() throws Exception {
    try (MockedStatic<JsonUtil> jsonUtilMocked = mockStatic(JsonUtil.class)) {
      String responseJson =
          "{\"flags\":[{\"name\":\"test_feature\",\"value\":\"true\"}],\"ttl_seconds\":60}";
      when(httpResponseMock.getStatusLine()).thenReturn(statusLineMock);
      when(statusLineMock.getStatusCode()).thenReturn(200);
      when(httpResponseMock.getEntity()).thenReturn(httpEntityMock);
      when(httpEntityMock.getContent())
          .thenReturn(new ByteArrayInputStream(responseJson.getBytes()));
      when(httpClientMock.execute(any(HttpGet.class))).thenReturn(httpResponseMock);
      FeatureFlagsResponse response = createFeatureFlagsResponse(FEATURE_FLAG_NAME, "true", 60);
      jsonUtilMocked.when(JsonUtil::getMapper).thenReturn(objectMapperMock);
      when(objectMapperMock.readValue(anyString(), eq(FeatureFlagsResponse.class)))
          .thenReturn(response);
      HttpGet request = new HttpGet(FEATURE_FLAGS_ENDPOINT);
      context.fetchAndSetFlagsFromServer(httpClientMock, request);
      assertTrue(context.isFeatureEnabled(FEATURE_FLAG_NAME));
      verify(httpClientMock).execute(request);
    }
  }

  @Test
  void testFetchAndSetFlagsFromServer_HttpError() throws IOException, DatabricksHttpException {
    when(httpResponseMock.getStatusLine()).thenReturn(statusLineMock);
    when(statusLineMock.getStatusCode()).thenReturn(500);
    when(httpClientMock.execute(any(HttpGet.class))).thenReturn(httpResponseMock);
    HttpGet request = new HttpGet(FEATURE_FLAGS_ENDPOINT);
    context.fetchAndSetFlagsFromServer(httpClientMock, request);
    assertFalse(context.isFeatureEnabled(FEATURE_FLAG_NAME));
    verify(httpClientMock).execute(request);
  }

  @Test
  void testFetchAndSetFlagsFromServer_EmptyFlags() throws Exception {
    try (MockedStatic<JsonUtil> jsonUtilMocked = mockStatic(JsonUtil.class)) {
      String responseJson = "{\"flags\":[],\"ttl_seconds\":300}";
      when(httpResponseMock.getStatusLine()).thenReturn(statusLineMock);
      when(statusLineMock.getStatusCode()).thenReturn(200);
      when(httpResponseMock.getEntity()).thenReturn(httpEntityMock);
      when(httpEntityMock.getContent())
          .thenReturn(new ByteArrayInputStream(responseJson.getBytes()));
      when(httpClientMock.execute(any(HttpGet.class))).thenReturn(httpResponseMock);
      FeatureFlagsResponse response = createFeatureFlagsResponseWithNoFlags(300);
      jsonUtilMocked.when(JsonUtil::getMapper).thenReturn(objectMapperMock);
      when(objectMapperMock.readValue(anyString(), eq(FeatureFlagsResponse.class)))
          .thenReturn(response);
      HttpGet request = new HttpGet(FEATURE_FLAGS_ENDPOINT);
      context.fetchAndSetFlagsFromServer(httpClientMock, request);
      assertFalse(context.isFeatureEnabled(FEATURE_FLAG_NAME));
      verify(httpClientMock).execute(request);
    }
  }

  @Test
  void testFetchAndSetFlagsFromServer_NullFlags() throws Exception {
    try (MockedStatic<JsonUtil> jsonUtilMocked = mockStatic(JsonUtil.class)) {
      String responseJson = "{\"ttl_seconds\":300}";
      when(httpResponseMock.getStatusLine()).thenReturn(statusLineMock);
      when(statusLineMock.getStatusCode()).thenReturn(200);
      when(httpResponseMock.getEntity()).thenReturn(httpEntityMock);
      when(httpEntityMock.getContent())
          .thenReturn(new ByteArrayInputStream(responseJson.getBytes()));
      when(httpClientMock.execute(any(HttpGet.class))).thenReturn(httpResponseMock);
      FeatureFlagsResponse response = createFeatureFlagsResponseNullFlags(300);
      jsonUtilMocked.when(JsonUtil::getMapper).thenReturn(objectMapperMock);
      when(objectMapperMock.readValue(anyString(), eq(FeatureFlagsResponse.class)))
          .thenReturn(response);
      HttpGet request = new HttpGet(FEATURE_FLAGS_ENDPOINT);
      context.fetchAndSetFlagsFromServer(httpClientMock, request);
      assertFalse(context.isFeatureEnabled(FEATURE_FLAG_NAME));
      verify(httpClientMock).execute(request);
    }
  }

  @Test
  void testIsFeatureEnabled() {
    // Test with valid boolean values
    Map<String, String> flags = new HashMap<>();
    flags.put("flag1", "true");
    flags.put("flag2", "false");
    context = new DatabricksDriverFeatureFlagsContext(connectionContextMock, flags);
    assertTrue(context.isFeatureEnabled("flag1"));
    assertFalse(context.isFeatureEnabled("flag2"));

    // Test with invalid values
    flags.put("flag3", "invalid");
    flags.put("flag4", "yes");
    context = new DatabricksDriverFeatureFlagsContext(connectionContextMock, flags);
    assertFalse(context.isFeatureEnabled("flag3"));
    assertFalse(context.isFeatureEnabled("flag4"));

    // Test with non-existent flag
    assertFalse(context.isFeatureEnabled("nonexistent"));
  }
}
