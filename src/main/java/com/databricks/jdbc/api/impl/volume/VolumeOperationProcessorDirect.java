package com.databricks.jdbc.api.impl.volume;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.sqlexec.HttpHeader;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.message.BasicHeader;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class VolumeOperationProcessorDirect {
    private static final JdbcLogger LOGGER =
            JdbcLoggerFactory.getLogger(VolumeOperationProcessorDirect.class);

    private final String operationUrl;
    private final String localFilePath;
    private final List<BasicHeader> headers;
    private final IDatabricksSession session;
    private final IDatabricksHttpClient databricksHttpClient;

    //this.httpClient = DatabricksHttpClient.getInstance(session.getConnectionContext());
    public VolumeOperationProcessorDirect(String operationUrl, String localFilePath, List<BasicHeader> headers, IDatabricksSession session) {
        this.operationUrl = operationUrl;
        this.localFilePath = localFilePath;
        this.headers=headers;
        this.session=session;
        this.databricksHttpClient = DatabricksHttpClient.getInstance(session.getConnectionContext());
    }

    private boolean isSuccessfulHttpResponse(CloseableHttpResponse response) {
        return response.getStatusLine().getStatusCode() >= 200
                && response.getStatusLine().getStatusCode() < 300;
    }

    public void executePutOperation() {
        System.out.println("Line  47");
        HttpPut httpPut = new HttpPut(operationUrl);
//        headers.forEach(httpPut::addHeader);

        System.out.println("Line  49");
        // Set the FileEntity as the request body
        File file = new File(localFilePath);
        httpPut.setEntity(new FileEntity(file, ContentType.DEFAULT_BINARY));

        System.out.println(localFilePath);
        // Execute the request
        try (CloseableHttpResponse response = databricksHttpClient.execute(httpPut)) {
            // Process the response
            System.out.println("Reponse: "+response);
            if (isSuccessfulHttpResponse(response)) {
                LOGGER.debug(String.format("Successfully uploaded file: {%s}", localFilePath));
            } else {
                LOGGER.error(
                        String.format(
                                "Failed to upload file {%s} with error code: {%s}",
                                localFilePath, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException | DatabricksHttpException e) {
            LOGGER.error(
                    String.format(
                            "Failed to upload file {%s} with error {%s}", localFilePath, e.getMessage()));
        }
    }
}
