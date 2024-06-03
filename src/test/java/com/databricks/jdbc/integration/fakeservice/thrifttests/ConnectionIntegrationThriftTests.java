package com.databricks.jdbc.integration.fakeservice.thrifttests;

import com.databricks.jdbc.integration.fakeservice.tests.ConnectionIntegrationTests;
import org.junit.jupiter.api.BeforeAll;

public class ConnectionIntegrationThriftTests extends ConnectionIntegrationTests {

  @BeforeAll
  static void beforeAll() {
    setSqlExecApiTargetUrl("https://e2-dogfood.staging.cloud.databricks.com");
  }

  @Override
  protected String getUnsuccessfulConnectionMessage() {
    return "Error while receiving response from Thrift server";
  }
}
