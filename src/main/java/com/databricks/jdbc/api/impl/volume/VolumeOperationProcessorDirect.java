package com.databricks.jdbc.api.impl.volume;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.common.util.HttpUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksVolumeOperationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.File;
import java.io.IOException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;

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
      String operationUrl, String localFilePath, IDatabricksSession session) {
    this.operationUrl = operationUrl;
    this.localFilePath = localFilePath;
    this.databricksHttpClient =
        DatabricksHttpClientFactory.getInstance().getClient(session.getConnectionContext());
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
}
