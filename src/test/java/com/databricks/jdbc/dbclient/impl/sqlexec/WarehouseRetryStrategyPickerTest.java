package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.retry.IdempotentRequestRetryStrategy;
import com.databricks.sdk.core.retry.NonIdempotentRequestRetryStrategy;
import com.databricks.sdk.core.retry.RequestBasedRetryStrategyPicker;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class WarehouseRetryStrategyPickerTest {
  private static final String HOST = "https://workspace.example.com";
  private static final String SESSIONS = "/api/2.0/sql/warehouses/warehouse/sessions";
  private final WarehouseRetryStrategyPicker picker = new WarehouseRetryStrategyPicker(HOST);

  @ParameterizedTest
  @CsvSource({
    "POST, ''",
    "POST, /",
    "DELETE, /session",
    "GET, /session/statements/statement",
    "DELETE, /session/statements/statement",
    "GET, /session/statements/statement/status",
    "GET, /session/statements/statement/result/chunks/0",
    "GET, /session/statements/statement/result/chunks/1/data"
  })
  void warehouseIdempotentRequests(String method, String suffix) {
    for (String prefix : new String[] {"", HOST}) {
      Request request = new Request(method, prefix + SESSIONS + suffix);
      assertInstanceOf(IdempotentRequestRetryStrategy.class, picker.getRetryStrategy(request));
      assertEquals(prefix + SESSIONS + suffix, request.getUrl());
    }
  }

  @ParameterizedTest
  @CsvSource({
    "POST, /session/statements",
    "POST, /session/statements/",
    "POST, /session/statements/statement/cancel",
    "GET, ''",
    "GET, /session",
    "PUT, /session",
    "DELETE, /session/statements",
    "DELETE, /session/statements/statement/result/chunks/0",
    "GET, /session/statements/statement/unknown",
    "POST, /extra",
    "POST, -extra",
    "POST, //"
  })
  void otherWarehouseRequestsRemainNonIdempotent(String method, String suffix) {
    for (String prefix : new String[] {"", HOST}) {
      assertInstanceOf(
          NonIdempotentRequestRetryStrategy.class,
          picker.getRetryStrategy(new Request(method, prefix + SESSIONS + suffix)));
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://other.example.com",
        "https://workspaceXexample.com",
        "https://workspace.example.com.evil.com",
        "https://evil.com/https://workspace.example.com",
        "http://workspace.example.com"
      })
  void warehouseRequestsMustMatchConfiguredHost(String host) {
    assertInstanceOf(
        NonIdempotentRequestRetryStrategy.class,
        picker.getRetryStrategy(new Request(Request.POST, host + SESSIONS)));
  }

  @ParameterizedTest
  @CsvSource({
    "POST, /api/2.0/sql/sessions/",
    "POST, /api/2.0/sql/statements/sessions/",
    "DELETE, /api/2.0/sql/sessions/session",
    "GET, /api/2.0/sql/statements/statement/status",
    "DELETE, /api/2.0/sql/statements/statement",
    "GET, /api/2.0/sql/statements/statement/result/chunks/0",
    "POST, /api/2.0/sql/statements/",
    "GET, /api/2.0/clusters/list"
  })
  void otherRequestsUseSdkClassification(String method, String path) {
    RequestBasedRetryStrategyPicker sdk = new RequestBasedRetryStrategyPicker(HOST);
    for (String prefix : new String[] {"", HOST}) {
      Request request = new Request(method, prefix + path);
      assertEquals(
          sdk.getRetryStrategy(request).getClass(), picker.getRetryStrategy(request).getClass());
    }
  }
}
