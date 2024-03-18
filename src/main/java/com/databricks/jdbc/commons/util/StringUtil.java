package com.databricks.jdbc.commons.util;

import static com.databricks.jdbc.client.impl.sdk.helper.MetadataResultSetBuilder.getSuccessResponseForGet;
import static com.databricks.jdbc.client.impl.sdk.helper.MetadataResultSetBuilder.getSuccessResponseForPut;

import com.databricks.jdbc.client.impl.thrift.generated.TColumn;
import com.databricks.jdbc.client.impl.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.client.sqlexec.ExecuteStatementResponse;
import com.databricks.jdbc.core.DatabricksResultSet;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class StringUtil {
  static String PATH_REGEX = "'(/[^']*)'";

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

  public static boolean isDeleteCommand(String sql) {
    return sql.toUpperCase().startsWith("DELETE");
  }

  public static String getPresignedUrl(ExecuteStatementResponse response) {
    Iterator<String> iterator = response.getResult().getDataArray().iterator().next().iterator();
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

  public static Map<String, String> parseHeaders(List<String> headerStrings) {
    Map<String, String> headers = new HashMap<>();
    Pattern pattern = Pattern.compile("\\{(.*?)\\}");
    for (String headerString : headerStrings) {
      Matcher matcher = pattern.matcher(headerString);
      if (matcher.find()) {
        String[] keyValue = matcher.group(1).split(":");
        if (keyValue.length == 2) {
          headers.put(keyValue[0].trim(), keyValue[1].trim());
        }
      }
    }
    System.out.println(" here is headers " + headers.toString());
    return headers;
  }

  private static Map<String, String> getHeaders(TFetchResultsResp response) {
    TColumn tColumn = response.getResults().getColumns().get(2);
    return parseHeaders(tColumn.getStringVal().getValues());
  }

  public static String getFilePathFromPUTSql(String sql) {
    Matcher matcher = Pattern.compile(PATH_REGEX).matcher(sql);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return "no path";
  }

  public static String getFilePathFromGETSql(String sql) {
    Matcher matcher = Pattern.compile(PATH_REGEX).matcher(sql);
    matcher.find();
    matcher.find();
    return matcher.group(1);
  }

  private static void uploadFileToS3UsingPresignedUrl(String presignedUrl, String filePath) {
    uploadFileToS3UsingPresignedUrl(presignedUrl, filePath, Collections.emptyMap());
  }

  private static void uploadFileToS3UsingPresignedUrl(
      String presignedUrl, String filePath, Map<String, String> headers) {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      byte[] data = Files.readAllBytes(Paths.get(filePath));
      HttpPut putRequest = new HttpPut(presignedUrl);
      headers.forEach(putRequest::setHeader);
      ByteArrayEntity entity = new ByteArrayEntity(data);
      putRequest.setEntity(entity);
      HttpResponse response = httpClient.execute(putRequest);
      System.out.println("response " + response.toString());
    } catch (IOException e) {
      System.out.println("HERE we go again: " + e.toString());
    }
  }

  public static DatabricksResultSet uploadFileAndGetResultSet(
      TFetchResultsResp response, String sql) throws SQLException {
    System.out.println("here is response : " + response.toString());
    String localFilePath = getFilePathFromPUTSql(sql);
    String presignedURL = getPresignedUrl(response);
    System.out.printf(
        "Here is the local path {%s}, and the presigned URL {%s}.%n", localFilePath, presignedURL);
    uploadFileToS3UsingPresignedUrl(presignedURL, localFilePath, getHeaders(response));
    return getSuccessResponseForPut();
  }

  public static DatabricksResultSet uploadFileAndGetResultSet(
      ExecuteStatementResponse response, String sql) throws SQLException {
    System.out.println("here is response : " + response.toString());
    String localFilePath = getFilePathFromPUTSql(sql);
    String presignedURL = getPresignedUrl(response);
    System.out.printf(
        "Here is the local path {%s}, and the presigned URL {%s}.%n", localFilePath, presignedURL);
    uploadFileToS3UsingPresignedUrl(presignedURL, localFilePath);
    return getSuccessResponseForPut();
  }

  public static DatabricksResultSet downloadFile(TFetchResultsResp response, String sql)
      throws SQLException {
    System.out.println("here is response : " + response.toString());
    String localFilePath = getFilePathFromGETSql(sql);
    String presignedURL = getPresignedUrl(response);
    System.out.printf(
        "Here is the local path {%s}, and the presigned URL {%s}.%n", localFilePath, presignedURL);
    downloadFile(presignedURL, localFilePath, getHeaders(response));
    return getSuccessResponseForGet();
  }

  private static void downloadFile(String presignedUrl, String localFile) {
    downloadFile(presignedUrl, localFile, Collections.emptyMap());
  }

  public static void downloadFile(
      String presignedUrl, String localFile, Map<String, String> headers) {
    HttpURLConnection connection = null;
    try {
      URL url = new URL(presignedUrl);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      headers.forEach(connection::setRequestProperty);
      int responseCode = connection.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        try (InputStream inputStream = connection.getInputStream();
            BufferedReader reader =
                new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            PrintWriter writer =
                new PrintWriter(
                    new OutputStreamWriter(
                        new FileOutputStream(localFile), StandardCharsets.UTF_8))) {
          String line;
          while ((line = reader.readLine()) != null) {
            writer.println(line);
          }
          System.out.println("File downloaded: " + localFile);
        } catch (IOException e) {
          System.err.println("Error writing file: " + e.toString());
        }
      } else {
        throw new IOException("HTTP error " + responseCode + " " + connection.getResponseMessage());
      }
    } catch (Exception e) {
      System.err.println("Error downloading file: " + e.toString());
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  public static DatabricksResultSet downloadFile(ExecuteStatementResponse response, String sql)
      throws SQLException {
    System.out.println("here is response : " + response.toString());
    String localFilePath = getFilePathFromGETSql(sql);
    String presignedURL = getPresignedUrl(response);
    System.out.printf(
        "Here is the local path {%s}, and the presigned URL {%s}.%n", localFilePath, presignedURL);
    downloadFile(presignedURL, localFilePath);
    return getSuccessResponseForGet();
  }

  public static DatabricksResultSet deleteFile(ExecuteStatementResponse response, String sql)
      throws SQLException {
    System.out.println("here is response : " + response.toString());
    String pathToBeDeleted = getFilePathFromPUTSql(sql);
    System.out.println("here is path to be deleted " + pathToBeDeleted);
    return getSuccessResponseForGet();
  }
}
