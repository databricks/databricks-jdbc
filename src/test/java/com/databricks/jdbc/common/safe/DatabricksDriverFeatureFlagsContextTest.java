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
import java.util.List;
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
      "https://test-host/api/2.0/connector-service/feature-flags/OSS_JDBC/3.4.3";

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
    Map<String, Object> flag = Map.of("name", flagName, "value", flagValue);
    Map<String, Object> response = Map.of("flags", List.of(flag), "ttlSeconds", ttlSeconds);
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
    Map<String, Object> flag = Map.of("name", flagName, "value", flagValue);
    Map<String, Object> response = Map.of("flags", List.of(flag), "ttlSeconds", ttlSeconds);
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
      assertTrue(context.getBoolean(FEATURE_FLAG_NAME));
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
      assertTrue(context.getBoolean(FEATURE_FLAG_NAME));
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
    assertFalse(context.getBoolean(FEATURE_FLAG_NAME));
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
      assertFalse(context.getBoolean(FEATURE_FLAG_NAME));
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
      assertFalse(context.getBoolean(FEATURE_FLAG_NAME));
      verify(httpClientMock).execute(request);
    }
  }

  @Test
  void testGetBoolean() {
    // Test with valid boolean values
    Map<String, String> flags = new HashMap<>();
    flags.put("flag1", "true");
    flags.put("flag2", "false");
    flags.put("mixedCase", "TrUe");
    context = new DatabricksDriverFeatureFlagsContext(connectionContextMock, flags);
    assertTrue(context.getBoolean("flag1"));
    assertFalse(context.getBoolean("flag2"));
    assertTrue(context.getBoolean("mixedCase"));

    // Test with invalid values
    flags.put("flag3", "invalid");
    flags.put("flag4", "yes");
    flags.put("flag5", "null");
    flags.put("flag6", "\"true\"");
    flags.put("flag7", "");
    flags.put("flag8", " true ");
    context = new DatabricksDriverFeatureFlagsContext(connectionContextMock, flags);
    for (String name :
        new String[] {"flag3", "flag4", "flag5", "flag6", "flag7", "flag8", "nonexistent", ""}) {
      assertFalse(context.getBoolean(name));
    }
    assertThrows(NullPointerException.class, () -> context.getBoolean(null));
  }

  @Test
  void testTypedGetters() {
    context =
        new DatabricksDriverFeatureFlagsContext(
            connectionContextMock,
            Map.of(
                "boolean", "true",
                "int32", Integer.toString(Integer.MIN_VALUE),
                "int64", Long.toString(Long.MAX_VALUE),
                "double", "3.5",
                "string", "\"hello\"",
                "string-list", "[\"a\",\"b\"]",
                "wrong-type", "\"true\"",
                "malformed", "not-json"));

    assertTrue(context.getBoolean("boolean"));
    assertEquals(Integer.MIN_VALUE, context.getInt32("int32").orElseThrow());
    assertEquals(Long.MAX_VALUE, context.getInt64("int64").orElseThrow());
    assertEquals(3.5, context.getDouble("double").orElseThrow());
    assertEquals("hello", context.getString("string").orElseThrow());
    assertEquals(List.of("a", "b"), context.getStringList("string-list").orElseThrow());
    assertFalse(context.getBoolean("wrong-type"));
    assertTrue(context.getString("malformed").isEmpty());
  }

  // ===== Additional Integration Tests =====

  @Test
  void testGetBooleanForSqlExecFlag() {
    Map<String, String> flags = new HashMap<>();
    flags.put("databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc", "true");
    context = new DatabricksDriverFeatureFlagsContext(connectionContextMock, flags);

    assertTrue(
        context.getBoolean(
            "databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc"));
  }

  @Test
  void testMultipleFeatureFlagsInResponse() throws Exception {
    try (MockedStatic<JsonUtil> jsonUtilMocked = mockStatic(JsonUtil.class)) {
      String responseJson =
          "{\"flags\":["
              + "{\"name\":\"flag1\",\"value\":\"true\"},"
              + "{\"name\":\"databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc\",\"value\":\"true\"},"
              + "{\"name\":\"flag3\",\"value\":\"false\"}"
              + "],\"ttl_seconds\":300}";

      when(httpResponseMock.getStatusLine()).thenReturn(statusLineMock);
      when(statusLineMock.getStatusCode()).thenReturn(200);
      when(httpResponseMock.getEntity()).thenReturn(httpEntityMock);
      when(httpEntityMock.getContent())
          .thenReturn(new ByteArrayInputStream(responseJson.getBytes()));
      when(httpClientMock.execute(any(HttpGet.class))).thenReturn(httpResponseMock);

      FeatureFlagsResponse response =
          new ObjectMapper().readValue(responseJson, FeatureFlagsResponse.class);
      jsonUtilMocked.when(JsonUtil::getMapper).thenReturn(objectMapperMock);
      when(objectMapperMock.readValue(anyString(), eq(FeatureFlagsResponse.class)))
          .thenReturn(response);

      HttpGet request = new HttpGet(FEATURE_FLAGS_ENDPOINT);
      context.fetchAndSetFlagsFromServer(httpClientMock, request);

      assertTrue(context.getBoolean("flag1"));
      assertTrue(
          context.getBoolean(
              "databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc"));
      assertFalse(context.getBoolean("flag3"));
    }
  }

  @Test
  void testFetchAndSetFlagsFromServer_404Error() throws IOException, DatabricksHttpException {
    when(httpResponseMock.getStatusLine()).thenReturn(statusLineMock);
    when(statusLineMock.getStatusCode()).thenReturn(404);
    when(httpClientMock.execute(any(HttpGet.class))).thenReturn(httpResponseMock);

    HttpGet request = new HttpGet(FEATURE_FLAGS_ENDPOINT);
    context.fetchAndSetFlagsFromServer(httpClientMock, request);

    // Should not throw, and feature should be disabled by default
    assertFalse(context.getBoolean(FEATURE_FLAG_NAME));
  }

  @Test
  void testFetchAndSetFlagsFromServer_403Error() throws IOException, DatabricksHttpException {
    when(httpResponseMock.getStatusLine()).thenReturn(statusLineMock);
    when(statusLineMock.getStatusCode()).thenReturn(403);
    when(httpClientMock.execute(any(HttpGet.class))).thenReturn(httpResponseMock);

    HttpGet request = new HttpGet(FEATURE_FLAGS_ENDPOINT);
    context.fetchAndSetFlagsFromServer(httpClientMock, request);

    assertFalse(context.getBoolean(FEATURE_FLAG_NAME));
  }

  @Test
  void testFetchAndSetFlagsFromServer_503Error() throws IOException, DatabricksHttpException {
    when(httpResponseMock.getStatusLine()).thenReturn(statusLineMock);
    when(statusLineMock.getStatusCode()).thenReturn(503);
    when(httpClientMock.execute(any(HttpGet.class))).thenReturn(httpResponseMock);

    HttpGet request = new HttpGet(FEATURE_FLAGS_ENDPOINT);
    context.fetchAndSetFlagsFromServer(httpClientMock, request);

    assertFalse(context.getBoolean(FEATURE_FLAG_NAME));
  }

  @Test
  void testGetBooleanCaseSensitive() {
    Map<String, String> flags = new HashMap<>();
    flags.put("TestFlag", "true");
    context = new DatabricksDriverFeatureFlagsContext(connectionContextMock, flags);

    assertTrue(context.getBoolean("TestFlag"));
    assertFalse(context.getBoolean("testflag"));
    assertFalse(context.getBoolean("TESTFLAG"));
  }
}
