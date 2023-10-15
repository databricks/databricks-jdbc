package com.databricks.jdbc.util;

import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.Asserts;
import org.apache.http.util.EntityUtils;

import java.awt.*;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.*;
import java.util.stream.Collectors;

/**
 * // TODO: implement logic to refresh tokens
 *
 * OAuth Authentication is done as follows:
 * 1. Get the IdP URL
 * 2. Get the OAuth config from the well known OAuth authorization server
 * 3. Create a random verifier and corresponding SHA256 challenger. Additionally, create a random state to prevent CSRF attacks
 * 4. Create a redirect URL on localhost where we would also host a server to handle callback
 * 5. Open the auth request URL in browser with the above parameters: challenge, state, redirect URL. User authenticates in browser using identity provider.
 * 6. Start a server on the redirect URL to look for a callback that contains the code
 * 7. Send a request to the OAuth config token endpoint with the code and verifier
 * 8. The token endpoint returns the access token that can be used for API requests
 */
public class OAuthAuthenticator {
    // OAuth config retrieved from IdP
    private final Map<String, Object> oAuthConfig;
    // TODO: should this instead be something specific to jdbc? Will have to be configured with the IdP
    private static final String CLIENT_ID = "databricks-cli";
    // Port for the server on localhost to handle callback
    private static final int LOCAL_PORT = 8020;
    HttpClient httpClient;
    // The Identity Provider URL
    String idpUrl;
    // Access token that can be used for API requests
    String accessToken;

    public OAuthAuthenticator(String hostname) throws IOException, URISyntaxException {
        this.httpClient = HttpClientBuilder.create().build();
        this.idpUrl = getIdpUrl(hostname);
        this.oAuthConfig = fetchWellKnownOAuthConfig(idpUrl);
    }

    /**
     * The method returns the URL to be used for redirections/callbacks in the OAuth authorization
     *
     * @return the redirection URL to be used
     */
    private String getRedirectUrl() throws URISyntaxException, MalformedURLException {
        return new URIBuilder().setScheme(DatabricksJdbcConstants.HTTP_SCHEMA)
                .setHost(DatabricksJdbcConstants.LOCALHOST).setPort(LOCAL_PORT)
                .build().toString();
    }

    /**
     * This method returns the token endpoint from the OAuth config
     *
     * @return the token endpoint that can be used to request for access tokens
     */
    private String getTokenEndpoint() {
        return (String) oAuthConfig.get("token_endpoint");
    }

    /**
     * This method returns the IdP URL given the hostname of the workspace
     *
     * @param hostname the workspace hostname
     * @return the IdP URL for the hostname
     */
    private String getIdpUrl(String hostname) throws URISyntaxException {
        return new URIBuilder(hostname).setScheme(DatabricksJdbcConstants.HTTPS_SCHEMA)
                .setPath("oidc").build().toString();
    }

    /**
     * This method fetches the well known OAuth config given the IdP URL
     *
     * @param idpUrl to fetch the OAuth config for
     * @return the OAuth config in the form of a java map
     * @throws IOException
     */
    private Map<String, Object> fetchWellKnownOAuthConfig(String idpUrl) throws IOException, URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(idpUrl);
        List<String> pathSegments = uriBuilder.getPathSegments();
        pathSegments.addAll(Arrays.asList(".well-known", "oauth-authorization-server"));
        String wellKnownConfigUrl = uriBuilder.setPathSegments(pathSegments).build().toString();
        HttpResponse response = this.httpClient.execute(new HttpGet(wellKnownConfigUrl));
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            throw new RuntimeException("Unable to fetch well known oauth config for: " + idpUrl);
        }
        return parseResponse(response);
    }

    /**
     * This methods returns the entity of a response in the form of a java map
     *
     * @param response to be parsed
     * @return the response entity in the form of a java map
     * @throws IOException
     */
    private Map<String, Object> parseResponse(HttpResponse response) throws IOException {
        String result = EntityUtils.toString(response.getEntity());
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(result, Map.class);
    }

    /**
     * This method generate URL safe random strings based on the number of bytes
     *
     * @param nBytes the number of bytes
     * @return the random string
     */
    private static String generateURLSafeRandomString(int nBytes) {
        byte[] randomBytes = new byte[nBytes];
        new Random(System.currentTimeMillis()).nextBytes(randomBytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes);
    }

    /**
     * This method generates a challenge in the form of the SHA256 hash of the input string
     *
     * @param verifier the random string to be used to generate SHA256 hash i.e. challenge
     * @return the SHA256 challenge generate form the verifier string
     * @throws NoSuchAlgorithmException
     */
    private static String getSHA256Challenge(String verifier) throws NoSuchAlgorithmException {
        byte[] bytes = verifier.getBytes(StandardCharsets.US_ASCII);
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        messageDigest.update(bytes, 0, bytes.length);
        byte[] hashedBytes = messageDigest.digest();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(hashedBytes);
    }

    /**
     * This method uses the code received in the callback to request for an access token from the token endpoint
     *
     * @param code the code received from the callback to the redirect uri
     * @param verifier the random string that corresponds to the challenge sent to the OAuth auth server
     * @return the access token received from the token endpoint
     * @throws IOException
     */
    private String tokenExchange(String code, String verifier) throws IOException, URISyntaxException {
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("grant_type", "authorization_code"));
        params.add(new BasicNameValuePair("code", code));
        params.add(new BasicNameValuePair("client_id", CLIENT_ID));
        params.add(new BasicNameValuePair("code_verifier", verifier));
        params.add(new BasicNameValuePair("redirect_uri", getRedirectUrl()));
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(params, StandardCharsets.UTF_8);

        HttpPost httpPost = new HttpPost(getTokenEndpoint());
        httpPost.setEntity(entity);

        HttpResponse response = httpClient.execute(httpPost);
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            throw new RuntimeException("Unable to perform token exchange");
        }
        Map<String, Object> properties = parseResponse(response);
        if (!properties.containsKey("access_token")) {
            throw new RuntimeException("Access token not returned by server");
        }
        return (String) properties.get("access_token");
    }

    /**
     * This method handles the callbacks done to the redirect uri with the code that can then be used to get access tokens
     *
     * @param exchange the http exchange with the code and state
     * @param codeVerifier the verifier string corresponding to the challenge, this must be sent to the token endpoint
     * @param expectedState the expected state value from the callback, to prevent CSRF attacks
     * @throws IOException
     */
    private void handleCallback(HttpExchange exchange, String codeVerifier, String expectedState) throws IOException, URISyntaxException {
        Map<String, String> queryParams = URLEncodedUtils.parse(exchange.getRequestURI(), StandardCharsets.UTF_8)
                .stream().collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue));

        // Verify the state parameter to prevent CSRF attacks.
        Asserts.check(queryParams.get("state").equals(expectedState), "Callback state does not match auth request state");

        this.accessToken = tokenExchange(queryParams.get("code"), codeVerifier);

        // Send a response to the browser indicating that the authorization was successful
        String message = "Authorization successful!";
        exchange.sendResponseHeaders(200, message.length());
        OutputStream outputStream = exchange.getResponseBody();
        outputStream.write(message.getBytes());
        outputStream.flush();
        outputStream.close();
    }

    /**
     * This method starts a server on the redirect url to handle the callback with the OAuth code
     *
     * @param verifier the random string corresponding to the challenge, it must be sent to the token endpoint
     * @param state the expected state from the callback to prevent CSRF attacks
     * @return the server object that was started
     * @throws IOException
     */
    private HttpServer startServer(String verifier, String state) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(LOCAL_PORT), 0);
        server.createContext(DatabricksJdbcConstants.SLASH, exchange -> {
            try {
                handleCallback(exchange, verifier, state);
            } catch (IOException | URISyntaxException e) {
                throw new RuntimeException("Failed to perform OAuth authentication while receiving callback\n" + e);
            }
        });
        server.setExecutor(null);
        server.start();
        return server;
    }

    /**
     * This method returns the auth request URL that requests a callback to the server with the code response
     *
     * @param verifier the random string to be used to generate the challenge
     * @param state the tandom string used to prevent CSRF attacks
     * @return the auth request URL
     * @throws NoSuchAlgorithmException
     */
    private String getAuthorizationRequestURL(String verifier, String state) throws NoSuchAlgorithmException, URISyntaxException, MalformedURLException {
        URIBuilder uriBuilder = new URIBuilder(idpUrl);
        List<String> pathSegments = uriBuilder.getPathSegments();
        pathSegments.addAll(Arrays.asList("oauth2", "v2.0", "authorize"));
        return uriBuilder
                .setPathSegments(pathSegments)
                .addParameter("response_type", "code")
                .addParameter("client_id", CLIENT_ID)
                .addParameter("redirect_uri", getRedirectUrl())
                // Using SHA-256 to generate challenge
                .addParameter("code_challenge", getSHA256Challenge(verifier))
                .addParameter("code_challenge_method", "S256")
                .addParameter("scope", "all-apis")
                .addParameter("state", state)
                .build().toString();
    }

    /**
     * THis method performs the OAuth authentication and returns an access token
     *
     * @return the access token to be used for API requests
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    public String getToken() throws IOException, NoSuchAlgorithmException, URISyntaxException, InterruptedException {
        // State is a randomly generated token to prevent CSRF attacks
        String state = generateURLSafeRandomString(16);
        // Random verifier token for OAuth challenge
        String verifier = generateURLSafeRandomString(32);
        String authRequestURL = getAuthorizationRequestURL(verifier, state);
        Desktop.getDesktop().browse(new URI(authRequestURL));

        HttpServer server = startServer(verifier, state);
        while (this.accessToken == null || this.accessToken.isEmpty()) {
            Thread.sleep(100);
        }
        server.stop(0);
        return this.accessToken;
    }
}
