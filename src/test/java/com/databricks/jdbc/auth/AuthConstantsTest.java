package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class AuthConstantsTest {

  @ParameterizedTest
  @CsvSource({"GRANT_TYPE_REFRESH_TOKEN_KEY, refresh_token", "GRANT_TYPE_KEY, grant_type"})
  void should_HaveExpectedConstantValue(String constantKey, String expectedValue) {
    String actual =
        "GRANT_TYPE_REFRESH_TOKEN_KEY".equals(constantKey)
            ? AuthConstants.GRANT_TYPE_REFRESH_TOKEN_KEY
            : AuthConstants.GRANT_TYPE_KEY;
    assertEquals(expectedValue, actual);
  }

  @Test
  void should_HaveNonEmptyConstants() {
    assertNotNull(AuthConstants.GRANT_TYPE_REFRESH_TOKEN_KEY);
    assertFalse(AuthConstants.GRANT_TYPE_REFRESH_TOKEN_KEY.isEmpty());
    assertNotNull(AuthConstants.GRANT_TYPE_KEY);
    assertFalse(AuthConstants.GRANT_TYPE_KEY.isEmpty());
  }
}
