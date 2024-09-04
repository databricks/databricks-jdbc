package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.SSLConfiguration;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.commons.CommonsHttpClient;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OAuthAuthenticator {

  private final IDatabricksConnectionContext connectionContext;
  private SSLContext sslContext;

  public OAuthAuthenticator(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;

    try {
      this.sslContext = SSLConfiguration.configureSslContext(
          connectionContext.getSSLKeyStorePath(),
          connectionContext.getSSLKeyStorePassword(),
          connectionContext.getSSLKeyStoreType(),
          connectionContext.getSSLTrustStorePath(),
          connectionContext.getSSLTrustStorePassword(),
          connectionContext.getSSLTrustStoreType(),
          connectionContext.getAllowSelfSignedCerts());
    } catch (Exception e) {
      this.sslContext = null;
    }
  }

  public WorkspaceClient getWorkspaceClient(DatabricksConfig databricksConfig)
      throws DatabricksParsingException {
    setupDatabricksConfig(databricksConfig);
    return new WorkspaceClient(databricksConfig);
  }

  public void setupDatabricksConfig(DatabricksConfig databricksConfig)
      throws DatabricksParsingException {
    if (this.connectionContext.isSSLEnabled()) {
        HttpClient httpClient = new SslDelegatingHttpClient(
            connectionContext.getIdleHttpConnectionExpiry(),
            sslContext);
        databricksConfig.setHttpClient(httpClient);
    }
    if (this.connectionContext.getAuthMech().equals(IDatabricksConnectionContext.AuthMech.PAT)) {
      setupAccessTokenConfig(databricksConfig);
    }
    // TODO(Madhav): Revisit these to set JDBC values
    else if (this.connectionContext
        .getAuthMech()
        .equals(IDatabricksConnectionContext.AuthMech.OAUTH)) {
      switch (this.connectionContext.getAuthFlow()) {
        case TOKEN_PASSTHROUGH:
          if (connectionContext.getOAuthRefreshToken() != null) {
            setupU2MRefreshConfig(databricksConfig);
          } else {
            setupAccessTokenConfig(databricksConfig);
          }
          break;
        case CLIENT_CREDENTIALS:
          setupM2MConfig(databricksConfig);
          break;
        case BROWSER_BASED_AUTHENTICATION:
          setupU2MConfig(databricksConfig);
          break;
      }
    } else {
      setupAccessTokenConfig(databricksConfig);
    }
  }

  public void setupU2MConfig(DatabricksConfig databricksConfig) throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.U2M_AUTH_TYPE)
        .setHost(connectionContext.getHostForOAuth())
        .setClientId(connectionContext.getClientId())
        .setClientSecret(connectionContext.getClientSecret())
        .setOAuthRedirectUrl(DatabricksJdbcConstants.U2M_AUTH_REDIRECT_URL);
    if (!databricksConfig.isAzure()) {
      databricksConfig.setScopes(connectionContext.getOAuthScopesForU2M());
    }
  }

  public void setupAccessTokenConfig(DatabricksConfig databricksConfig)
      throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE)
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getToken());
  }

  public void setupU2MRefreshConfig(DatabricksConfig databricksConfig)
      throws DatabricksParsingException {
    CredentialsProvider provider = new OAuthRefreshCredentialsProvider(connectionContext);
    databricksConfig
        .setHost(connectionContext.getHostForOAuth())
        .setAuthType(provider.authType())
        .setCredentialsProvider(provider)
        .setClientId(connectionContext.getClientId())
        .setClientSecret(connectionContext.getClientSecret());
  }

  public void setupM2MConfig(DatabricksConfig databricksConfig) throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.M2M_AUTH_TYPE)
        .setHost(connectionContext.getHostForOAuth())
        .setClientId(connectionContext.getClientId())
        .setClientSecret(connectionContext.getClientSecret());
    if (connectionContext.useJWTAssertion()) {
      databricksConfig.setCredentialsProvider(
          new PrivateKeyClientCredentialProvider(connectionContext));
    }
  }
}

class SslDelegatingHttpClient implements HttpClient {
  private final CommonsHttpClient delegate;  // Original CommonsHttpClient instance
  private final CloseableHttpClient sslClient;  // Custom SSL-enabled HttpClient

  public SslDelegatingHttpClient(int timeoutSeconds, SSLContext sslContext) {
    // Initialize the original CommonsHttpClient
    this.delegate = new CommonsHttpClient(timeoutSeconds);

    // Initialize a custom SSL-enabled HttpClient
    this.sslClient = createSslHttpClient(sslContext, timeoutSeconds);
  }

  @Override
  public Response execute(Request request) throws IOException {
    // Delegate the HTTP request handling to the original CommonsHttpClient
    return delegateWithCustomClient(request);
  }

  // This method delegates execution using the custom SSL HttpClient
  private Response delegateWithCustomClient(Request request) throws IOException {
    HttpUriRequest httpRequest = transformRequest(request);
    // Set the headers, timeouts, etc., as necessary
    request.getHeaders().forEach(httpRequest::setHeader);

    // Create a context to be used for capturing target URL later
    HttpContext context = new BasicHttpContext();

    // Execute using the SSL-enabled HTTP client
    CloseableHttpResponse httpResponse = sslClient.execute(httpRequest, context);

    // Process the response similarly to how CommonsHttpClient does
    return computeResponse(request, context, httpResponse);
  }

  // Recreate request transformation logic from CommonsHttpClient (if needed)
  private HttpUriRequest transformRequest(Request in) {
    switch (in.getMethod()) {
      case Request.GET:
        return new HttpGet(in.getUri());
      case Request.POST:
        return new HttpPost(in.getUri());
      default:
        throw new IllegalArgumentException("Unknown method: " + in.getMethod());
    }
  }

  // Recreate the response handling logic based on CommonsHttpClient
  private Response computeResponse(Request request, HttpContext context, CloseableHttpResponse response) throws IOException {
    HttpEntity entity = response.getEntity();
    StatusLine statusLine = response.getStatusLine();
    Map<String, List<String>> headers = Arrays.stream(response.getAllHeaders())
            .collect(Collectors.groupingBy(
                    header -> header.getName(),
                    Collectors.mapping(header -> header.getValue(), Collectors.toList())));

    // Extract the URL from the context
    URL targetUrl = getTargetUrl(context);

    if (entity == null) {
      response.close();
      return new Response(request, targetUrl, statusLine.getStatusCode(), statusLine.getReasonPhrase(), headers);
    }

    // Process the entity body if necessary
    String body = IOUtils.toString(entity.getContent(), "UTF-8");
    response.close();
    return new Response(request, targetUrl, statusLine.getStatusCode(), statusLine.getReasonPhrase(), headers, body);
  }

  // Recreate the getTargetUrl method from CommonsHttpClient
  private URL getTargetUrl(HttpContext context) {
    try {
      HttpHost targetHost = (HttpHost) context.getAttribute("http.target_host");
      HttpRequest httpRequest = (HttpRequest) context.getAttribute("http.request");
      URI uri = new URI(httpRequest.getRequestLine().getUri());
      return new URI(targetHost.getSchemeName(), null, targetHost.getHostName(), targetHost.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment()).toURL();
    } catch (URISyntaxException | IOException e) {
      throw new RuntimeException("Unable to get target URL", e);
    }
  }

  // Create a method to build your custom SSL HttpClient
  private CloseableHttpClient createSslHttpClient(SSLContext sslContext, int timeoutSeconds) {
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(100);  // Adjust pool size as needed

    // Build the SSL HttpClient with the provided SSLContext
    return HttpClientBuilder.create()
            .setConnectionManager(connectionManager)
            .setSSLContext(sslContext)
            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)  // Optional: disable hostname verification
            .build();
  }
}