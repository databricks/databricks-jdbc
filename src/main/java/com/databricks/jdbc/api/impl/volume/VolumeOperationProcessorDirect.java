package com.databricks.jdbc.api.impl.volume;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.HttpUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksVolumeOperationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.*;
import java.io.File;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.util.EntityUtils;

/**
 * VolumeOperationProcessorDirect is a class that performs the volume operation directly into the
 * DBFS using the pre signed url
 */
public class VolumeOperationProcessorDirect {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(VolumeOperationProcessorDirect.class);
  private final String operationUrl;
  private final String localFilePath;
  private final IDatabricksHttpClient databricksHttpClient;

  public VolumeOperationProcessorDirect(
      String operationUrl, String localFilePath, IDatabricksConnectionContext connectionContext) {
    this.operationUrl = operationUrl;
    this.localFilePath = localFilePath;
    this.databricksHttpClient =
        DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
  }

  public void executeGetOperation() throws DatabricksVolumeOperationException {
    HttpGet httpGet = new HttpGet(operationUrl);
    HttpEntity entity;

    // Copy the data in local file as requested by user
    File localFile = new File(localFilePath);
    if (localFile.exists()) {
      String errorMessage =
          String.format("Local file already exists for GET operation {%s}", localFilePath);
      LOGGER.error(errorMessage);
      throw new DatabricksVolumeOperationException(errorMessage);
    }

    try (CloseableHttpResponse response = databricksHttpClient.execute(httpGet)) {
      if (!HttpUtil.isSuccessfulHttpResponse(response)) {
        String errorMessage =
            String.format(
                "Failed to fetch content from volume with error {%s} for local file {%s}",
                response.getStatusLine().getStatusCode(), localFilePath);
        LOGGER.error(errorMessage);
        throw new DatabricksHttpException(errorMessage);
      }

      entity = response.getEntity();
      if (entity != null) {
        // Get the content of the HttpEntity
        InputStream inputStream = entity.getContent();
        // Create a FileOutputStream to write the content to a file
        try (FileOutputStream outputStream = new FileOutputStream(localFile)) {
          // Copy the content of the InputStream to the FileOutputStream
          byte[] buffer = new byte[1024];
          int length;
          while ((length = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, length);
          }
        } catch (FileNotFoundException e) {
          LOGGER.error("Local file path is invalid or a directory {%s}", localFilePath);
          throw e;
        } catch (IOException e) {
          LOGGER.error(
              e,
              "Failed to write to local file {%s} with error {%s}",
              localFilePath,
              e.getMessage());
          throw e;
        } finally {
          // It's important to consume the entity content fully and ensure the stream is closed
          EntityUtils.consume(entity);
        }
      }
    } catch (IOException | DatabricksHttpException e) {
      String errorMessage = String.format("Failed to download file : {%s} ", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(errorMessage, e);
    }
  }

  public void executePutOperation() throws DatabricksVolumeOperationException {
    HttpPut httpPut = new HttpPut(operationUrl);

    // Set the FileEntity as the request body
    File file = new File(localFilePath);
    httpPut.setEntity(new FileEntity(file, ContentType.DEFAULT_BINARY));
    // Execute the request
    try (CloseableHttpResponse response = databricksHttpClient.execute(httpPut)) {
      // Process the response
      if (HttpUtil.isSuccessfulHttpResponse(response)) {
        LOGGER.debug(String.format("Successfully uploaded file: {%s}", localFilePath));
      } else {
        LOGGER.error(
            String.format(
                "Failed to upload file {%s} with error code: {%s}",
                localFilePath, response.getStatusLine().getStatusCode()));
      }
    } catch (IOException | DatabricksHttpException e) {
      String errorMessage =
          String.format(
              "Failed to upload file {%s} with error {%s}", localFilePath, e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(errorMessage, e);
    }
  }

  public void executeDeleteOperation() throws DatabricksVolumeOperationException {
    HttpDelete httpDelete = new HttpDelete(operationUrl);

    try (CloseableHttpResponse response = databricksHttpClient.execute(httpDelete)) {
      if (HttpUtil.isSuccessfulHttpResponse(response)) {
        LOGGER.debug("Successfully deleted object");
      } else {
        String errorMessage =
            String.format(
                "Failed to delete object with error code: {%s}",
                response.getStatusLine().getStatusCode());
        LOGGER.error(errorMessage);
        throw new DatabricksHttpException(errorMessage);
      }
    } catch (DatabricksHttpException | IOException e) {
      String errorMessage =
          String.format("Failed to delete volume with error {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(errorMessage, e);
    }
  }
}
