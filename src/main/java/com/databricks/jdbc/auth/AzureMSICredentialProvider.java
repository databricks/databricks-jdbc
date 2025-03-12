package com.databricks.jdbc.auth;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.AZURE_MSI_AUTH_TYPE;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.HeaderFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpHeaders;

public class AzureMSICredentialProvider implements CredentialsProvider {
  IDatabricksHttpClient httpClient;
  String resourceId;
  String clientId;

  public AzureMSICredentialProvider(IDatabricksConnectionContext connectionContext) {
    this.httpClient = DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
    this.clientId = connectionContext.getNullableClientId();
    this.resourceId = connectionContext.getAzureWorkspaceResourceId();
  }

  @Override
  public String authType() {
    return AZURE_MSI_AUTH_TYPE;
  }

  @Override
  public HeaderFactory configure(DatabricksConfig databricksConfig) {
    AzureMSICredentials azureMSICredentials = new AzureMSICredentials(httpClient, clientId);

    return () -> {
      Map<String, String> headers = new HashMap<>();
      if (resourceId != null) {
        headers.put("X-Databricks-Azure-Workspace-Resource-Id", resourceId);
        headers.put(
            "X-Databricks-Azure-SP-Management-Token",
            azureMSICredentials.getManagementEndpointToken().getAccessToken());
      }
      headers.put(
          HttpHeaders.AUTHORIZATION, "Bearer " + azureMSICredentials.getToken().getAccessToken());

      return headers;
    };
  }
}
