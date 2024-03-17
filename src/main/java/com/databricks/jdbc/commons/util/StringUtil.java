package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.client.impl.thrift.generated.TColumn;
import com.databricks.jdbc.client.impl.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.client.sqlexec.ExecuteStatementResponse;
import com.databricks.jdbc.core.DatabricksResultSet;
import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class StringUtil {
  static String PUT_PATH_REGEX = "'(/[^']*)'";

  public static String getProcessedEscapeSequence(String sql) {
    // Replace JDBC escape sequences;
    // TODO : Check if some other escape sequence is required by us.
    sql =
        sql.replaceAll("\\{d '([0-9]{4}-[0-9]{2}-[0-9]{2})'\\}", "DATE '$1'") // DATE
            .replaceAll("\\{t '([0-9]{2}:[0-9]{2}:[0-9]{2})'\\}", "TIME '$1'") // TIME
            .replaceAll(
                "\\{ts '([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?)'\\}",
                "TIMESTAMP '$1'") // TIMESTAMP
            .replaceAll("\\{fn ([^}]*)\\}", "$1"); // JDBC function escape sequence
    return sql;
  }

  public static boolean isPutCommand(String sql) {
    return sql.toUpperCase().startsWith("PUT");
  }

  public static boolean isGetCommand(String sql) {
    return sql.toUpperCase().startsWith("GET");
  }

  public static String getPresignedUrl(ExecuteStatementResponse response) {
    Collection<Collection<String>> dataArray = response.getResult().getDataArray();
    Iterator<String> iterator = dataArray.iterator().next().iterator();
    iterator.next();
    return iterator.next();
  }

  public static String getPresignedUrl(TFetchResultsResp response) {
    TColumn column = response.getResults().getColumns().get(1);
    if (column.isSetStringVal() && !column.getStringVal().getValues().isEmpty()) {
      return column.getStringVal().getValues().get(0);
    }
    return "noURL";
  }

  public static String getFilePathFromPUTSql(String sql) {
    Matcher matcher = Pattern.compile(PUT_PATH_REGEX).matcher(sql);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return "no path";
  }

  public static String getFilePathFromGETSql(String sql) {
    Matcher matcher = Pattern.compile(PUT_PATH_REGEX).matcher(sql);
    matcher.find();
    matcher.find();
    return matcher.group(1);
  }

  private static void uploadFileToS3UsingPresignedUrl(String presignedUrl, String filePath) {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      byte[] data = Files.readAllBytes(Paths.get(filePath));
      HttpPut putRequest = new HttpPut(presignedUrl);
      putRequest.setHeader("Content-Type", "text/csv");
      ByteArrayEntity entity = new ByteArrayEntity(data);
      entity.setContentType("text/csv");
      putRequest.setEntity(entity);
      HttpResponse response = httpClient.execute(putRequest);
      String content = EntityUtils.toString(response.getEntity());
      System.out.println("here " + content);
      System.out.println("response " + response.toString());
    } catch (IOException e) {
      System.out.println("HERE we go again: " + e.toString());
    }
  }

  public static DatabricksResultSet uploadFileAndGetResultSet(
      TFetchResultsResp response, String sql) {
    System.out.println("here is response : " + response.toString());
    String localFilePath = getFilePathFromPUTSql(sql);
    String presignedURL = getPresignedUrl(response);
    System.out.printf(
        "Here is the local path {%s}, and the presigned URL {%s}.%n", localFilePath, presignedURL);
    uploadFileToS3UsingPresignedUrl(presignedURL, localFilePath);
    return null;
  }

  public static DatabricksResultSet uploadFileAndGetResultSet(
      ExecuteStatementResponse response, String sql) {
    System.out.println("here is response : " + response.toString());
    String localFilePath = getFilePathFromPUTSql(sql);
    String presignedURL = getPresignedUrl(response);
    System.out.printf(
        "Here is the local path {%s}, and the presigned URL {%s}.%n", localFilePath, presignedURL);
    uploadFileToS3UsingPresignedUrl(presignedURL, localFilePath);
    return null;
  }

  public static DatabricksResultSet downloadFile(TFetchResultsResp response, String sql) {
    System.out.println("here is response : " + response.toString());
    String localFilePath = getFilePathFromGETSql(sql);
    String presignedURL = getPresignedUrl(response);
    System.out.printf(
        "Here is the local path {%s}, and the presigned URL {%s}.%n", localFilePath, presignedURL);
    downloadFile(presignedURL, localFilePath);
    return null;
  }

  private static void downloadFile(String presignedUrl, String outputPath) {
    HttpURLConnection connection = null;
    try {
      URL url = new URL(presignedUrl);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      int responseCode = connection.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new IOException(
            "Server returned HTTP response code: " + responseCode + " for URL: " + presignedUrl);
      }
      try (BufferedInputStream in = new BufferedInputStream(connection.getInputStream());
          FileOutputStream fileOutputStream = new FileOutputStream(outputPath)) {
        byte[] dataBuffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = in.read(dataBuffer, 0, 4096)) != -1) {
          fileOutputStream.write(dataBuffer, 0, bytesRead);
        }
      }
    } catch (IOException e) {
      System.out.println("io error " + e.toString());
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  public static DatabricksResultSet downloadFile(ExecuteStatementResponse response, String sql) {
    System.out.println("here is response : " + response.toString());
    String localFilePath = getFilePathFromGETSql(sql);
    String presignedURL = getPresignedUrl(response);
    System.out.printf(
        "Here is the local path {%s}, and the presigned URL {%s}.%n", localFilePath, presignedURL);
    downloadFile(presignedURL, localFilePath);
    return null;
  }
}
