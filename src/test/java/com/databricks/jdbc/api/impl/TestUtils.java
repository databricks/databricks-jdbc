package com.databricks.jdbc.api.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class TestUtils {
  // Helper method to convert InputStream to String in Java 8
  public static String convertInputStreamToString(InputStream inputStream) throws IOException {
    StringBuilder stringBuilder = new StringBuilder();
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    while ((line = reader.readLine()) != null) {
      stringBuilder.append(line);
    }
    return stringBuilder.toString();
  }
}
