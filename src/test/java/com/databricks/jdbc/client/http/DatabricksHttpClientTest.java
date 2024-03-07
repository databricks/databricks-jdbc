package com.databricks.jdbc.client.http;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.jupiter.api.Test;

public class DatabricksHttpClientTest {
  @Test
  public void testSetProxyDetailsIntoHttpClient() {
    HttpClientBuilder builder = HttpClientBuilder.create();
    assertDoesNotThrow(
        () ->
            DatabricksHttpClient.setProxyDetailsInHttpClient(
                builder, "proxyHost", 8080, true, "proxyUser", "proxyPassword"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DatabricksHttpClient.setProxyDetailsInHttpClient(
                builder, "proxyHost", 8080, true, null, "proxyPassword"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DatabricksHttpClient.setProxyDetailsInHttpClient(
                builder, null, 8080, true, "user", "proxyPassword"));
  }
}
