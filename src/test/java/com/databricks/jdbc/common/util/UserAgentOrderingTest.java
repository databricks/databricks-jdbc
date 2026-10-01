package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class UserAgentOrderingTest {
  private static final String BASE = "Driver/1 sdk/1 jvm/17 os/linux";

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void ordersSeaUserAgent(
      String scenario, String original, String customerUserAgent, String expected) {
    assertEquals(expected, UserAgentManager.orderSeaUserAgent(original, customerUserAgent));
  }

  private static Stream<Arguments> cases() {
    return Stream.of(
        arguments(
            "registered entry",
            BASE + " Java/SQLExecHttpClient ThoughtSpot/version auth/pat",
            "ThoughtSpot",
            BASE + " ThoughtSpot/version Java/SQLExecHttpClient auth/pat"),
        arguments(
            "switch from Thrift without SEA marker",
            BASE + " Java/THttpClient ThoughtSpot/version auth/pat",
            "ThoughtSpot",
            BASE + " ThoughtSpot/version Java/SQLExecHttpClient Java/THttpClient auth/pat"),
        arguments(
            "SDK CI and agent segments",
            BASE + " cicd/github agent/codex Java/SQLExecHttpClient ThoughtSpot/version auth/pat",
            "ThoughtSpot",
            BASE + " ThoughtSpot/version Java/SQLExecHttpClient cicd/github agent/codex auth/pat"),
        arguments(
            "unregistered entry",
            BASE + " EarlierApp/1.0 Java/SQLExecHttpClient auth/pat",
            "Unregistered/1.0",
            BASE + " Java/SQLExecHttpClient EarlierApp/1.0 auth/pat"),
        arguments(
            "entry equals SEA marker",
            BASE + " Java/SQLExecHttpClient auth/pat",
            "Java/SQLExecHttpClient",
            BASE + " Java/SQLExecHttpClient auth/pat"),
        arguments(
            "missing OS segment",
            "Driver/1 sdk/1 Java/SQLExecHttpClient auth/pat",
            "ThoughtSpot",
            "Driver/1 sdk/1 Java/SQLExecHttpClient auth/pat"));
  }
}
