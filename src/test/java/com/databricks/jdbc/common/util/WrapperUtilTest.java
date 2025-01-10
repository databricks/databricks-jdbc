package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;

public class WrapperUtilTest {
  @Test
  public void testIsWrapperFor() {
    String str = "test";
    assertTrue(
        WrapperUtil.isWrapperFor(CharSequence.class, str),
        "String should be an instance of CharSequence");
    assertFalse(
        WrapperUtil.isWrapperFor(Number.class, str), "String should not be an instance of Number");
  }

  @Test
  public void testUnwrap() throws SQLException {
    String str = "test";
    // Check if it can be unwrapped to a CharSequence
    CharSequence charSequence = WrapperUtil.unwrap(CharSequence.class, str, null);
    assertInstanceOf(
        CharSequence.class, charSequence, "String should be unwrapped to a CharSequence");
    // Check if trying to unwrap it to a Number throws a SQLException
    assertThrows(
        SQLException.class,
        () -> WrapperUtil.unwrap(Number.class, str, null),
        "Unwrapping String to a Number should throw SQLException");
  }
}
