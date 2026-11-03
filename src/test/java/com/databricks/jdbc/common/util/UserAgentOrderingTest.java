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
      String scenario, String original, String customerSegment, String expected) {
    assertEquals(expected, UserAgentManager.orderSeaUserAgent(original, customerSegment));
  }

  private static Stream<Arguments> cases() {
    return Stream.of(
        arguments(
            "registered entry",
            BASE + " Java/SQLExecHttpClient ThoughtSpot/version auth/pat",
            "ThoughtSpot/version",
            BASE + " ThoughtSpot/version Java/SQLExecHttpClient auth/pat"),
        arguments(
            "switch from Thrift without SEA marker",
            BASE + " Java/THttpClient ThoughtSpot/version auth/pat",
            "ThoughtSpot/version",
            BASE + " ThoughtSpot/version Java/SQLExecHttpClient Java/THttpClient auth/pat"),
        arguments(
            "SDK CI and agent segments",
            BASE + " cicd/github agent/codex Java/SQLExecHttpClient ThoughtSpot/version auth/pat",
            "ThoughtSpot/version",
            BASE + " ThoughtSpot/version Java/SQLExecHttpClient cicd/github agent/codex auth/pat"),
        arguments(
            "current entry absent from global header",
            BASE + " EarlierApp/1.0 Java/SQLExecHttpClient auth/pat",
            "Unregistered/1.0",
            BASE + " Unregistered/1.0 Java/SQLExecHttpClient EarlierApp/1.0 auth/pat"),
        arguments(
            "no customer entry",
            BASE + " EarlierApp/1.0 Java/SQLExecHttpClient auth/pat",
            null,
            BASE + " EarlierApp/1.0 Java/SQLExecHttpClient auth/pat"),
        arguments(
            "entry equals SEA marker",
            BASE + " Java/SQLExecHttpClient auth/pat",
            "Java/SQLExecHttpClient",
            BASE + " Java/SQLExecHttpClient auth/pat"),
        arguments(
            "customer entry matches SDK prefix",
            BASE + " Java/SQLExecHttpClient auth/pat",
            "Driver/1",
            BASE + " Driver/1 Java/SQLExecHttpClient auth/pat"),
        arguments(
            "missing OS segment",
            "Driver/1 sdk/1 Java/SQLExecHttpClient auth/pat",
            "ThoughtSpot/version",
            "Driver/1 sdk/1 Java/SQLExecHttpClient auth/pat"));
  }
}
