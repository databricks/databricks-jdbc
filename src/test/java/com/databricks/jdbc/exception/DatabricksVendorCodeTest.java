package com.databricks.jdbc.exception;

import static org.junit.jupiter.api.Assertions.*;

import java.net.UnknownHostException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for DatabricksVendorCode vendor code detection functionality. */
class DatabricksVendorCodeTest {

  // Test getVendorCode(String exceptionMessage) method

  @Test
  void testGetVendorCodeWithNullMessage() {
    int result = DatabricksVendorCode.getVendorCode((String) null);
    assertEquals(0, result, "Should return 0 for null message");
  }

  @ParameterizedTest
  @MethodSource("provideValidVendorCodeMessages")
  void testGetVendorCodeWithValidMessages(String message, int expectedCode) {
    int result = DatabricksVendorCode.getVendorCode(message);
    assertEquals(expectedCode, result, "Should return correct vendor code for message: " + message);
  }

  @ParameterizedTest
  @MethodSource("provideInvalidVendorCodeMessages")
  void testGetVendorCodeWithInvalidMessages(String message) {
    int result = DatabricksVendorCode.getVendorCode(message);
    assertEquals(0, result, "Should return 0 for message without vendor codes: " + message);
  }

  @Test
  void testGetVendorCodeCaseInsensitive() {
    // Test case insensitivity - uppercase version should match
    String upperCaseMessage =
        "INVALID UID PARAMETER: Expected 'token' or omit UID parameter entirely";
    int result = DatabricksVendorCode.getVendorCode(upperCaseMessage);
    assertEquals(
        DatabricksVendorCode.INCORRECT_UID.getCode(),
        result,
        "Should be case insensitive and match uppercase version");

    String mixedCaseMessage = "Invalid Access Token";
    result = DatabricksVendorCode.getVendorCode(mixedCaseMessage);
    assertEquals(
        DatabricksVendorCode.INCORRECT_ACCESS_TOKEN.getCode(),
        result,
        "Should be case insensitive and match mixed case");
  }

  // Test getVendorCode(Throwable throwable) method

  @Test
  void testGetVendorCodeWithNullThrowable() {
    int result = DatabricksVendorCode.getVendorCode((Throwable) null);
    assertEquals(0, result, "Should return 0 for null throwable");
  }

  @Test
  void testGetVendorCodeWithThrowableContainingValidMessage() {
    Exception exception =
        new RuntimeException(
            "Invalid UID parameter: Expected 'token' or omit UID parameter entirely for authentication");
    int result = DatabricksVendorCode.getVendorCode(exception);
    assertEquals(
        DatabricksVendorCode.INCORRECT_UID.getCode(),
        result,
        "Should return INCORRECT_UID vendor code");
  }

  @Test
  void testGetVendorCodeWithThrowableWithoutValidMessage() {
    Exception exception = new RuntimeException("Some unrelated error message");
    int result = DatabricksVendorCode.getVendorCode(exception);
    assertEquals(0, result, "Should return 0 for throwable without vendor code message");
  }

  @Test
  void testGetVendorCodeWithThrowableChain() {
    // Create a chain of exceptions where the vendor code is in a nested cause
    Exception rootCause = new UnknownHostException("Host not found");
    Exception topLevel = new RuntimeException("Connection failed", rootCause);

    int result = DatabricksVendorCode.getVendorCode(topLevel);
    assertEquals(
        DatabricksVendorCode.INCORRECT_HOST.getCode(),
        result,
        "Should find vendor code in exception chain");
  }

  // Data providers for parameterized tests

  private static Stream<Arguments> provideValidVendorCodeMessages() {
    return Stream.of(
        Arguments.of(
            "Invalid UID parameter: Expected 'token' or omit UID parameter entirely",
            DatabricksVendorCode.INCORRECT_UID.getCode()),
        Arguments.of("Invalid access token", DatabricksVendorCode.INCORRECT_ACCESS_TOKEN.getCode()),
        Arguments.of(
            "java.net.UnknownHostException", DatabricksVendorCode.INCORRECT_HOST.getCode()),
        Arguments.of(
            "Error: Invalid UID parameter: Expected 'token' or omit UID parameter entirely",
            DatabricksVendorCode.INCORRECT_UID.getCode()),
        Arguments.of(
            "Connection failed due to Invalid access token error",
            DatabricksVendorCode.INCORRECT_ACCESS_TOKEN.getCode()));
  }

  private static Stream<Arguments> provideInvalidVendorCodeMessages() {
    return Stream.of(
        Arguments.of(""),
        Arguments.of("   "),
        Arguments.of("Some random error message"),
        Arguments.of("Invalid token"), // Similar but not exact match
        Arguments.of("Connection failed"),
        Arguments.of("Authentication error"));
  }
}
