package com.databricks.jdbc.httpReq;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import org.apache.http.client.utils.URIBuilder;

// TODO: Authenticate the user with the access token
public class HTTP_REQ {
  public static String sendPostRequest(
      String urlString, String accessToken, HashMap<String, Double> map) {
    StringBuilder result = new StringBuilder();
    try {
      // Convert the HashMap to JSON
      ObjectMapper objectMapper = new ObjectMapper();
      String jsonInputString = objectMapper.writeValueAsString(map);

      // Build the URL with the query parameter
      URIBuilder uriBuilder = new URIBuilder(urlString);
      uriBuilder.addParameter("metrics_map_string", jsonInputString);
      URL url = uriBuilder.build().toURL();

      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Authorization", "Bearer " + accessToken);

      // Read response
      BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
      String line;
      while ((line = br.readLine()) != null) {
        result.append(line);
      }
      br.close();
      conn.disconnect();

    } catch (Exception e) {
      e.printStackTrace();
    }
    return result.toString();
  }
}
