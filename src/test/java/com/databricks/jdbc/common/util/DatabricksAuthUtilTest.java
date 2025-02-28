package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.http.HttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DatabricksAuthUtilTest {
  private DatabricksConfig mockConfig;
  private HttpClient mockHttpClient;
  private static final String NEW_ACCESS_TOKEN = "new-access-token";
  private static final String HOST_URL = "http://localhost:8080";

  @BeforeEach
  public void setUp() {
    mockConfig = mock(DatabricksConfig.class);
    mockHttpClient = mock(HttpClient.class);

    when(mockConfig.getHost()).thenReturn(HOST_URL);
    when(mockConfig.getHttpClient()).thenReturn(mockHttpClient);
  }

  @Test
  public void testInitializeConfigWithToken() {
    DatabricksConfig newConfig =
        DatabricksAuthUtil.initializeConfigWithToken(NEW_ACCESS_TOKEN, mockConfig);

    assertNotNull(newConfig);
    assertEquals(HOST_URL, newConfig.getHost());
    assertEquals(mockHttpClient, newConfig.getHttpClient());
    assertEquals(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE, newConfig.getAuthType());
    assertEquals(NEW_ACCESS_TOKEN, newConfig.getToken());
  }
}
