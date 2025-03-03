package com.databricks.jdbc.integration.fakeservice;

import static com.github.tomakehurst.wiremock.common.Exceptions.throwUnchecked;
import static com.github.tomakehurst.wiremock.common.ProxySettings.NO_PROXY;
import static com.github.tomakehurst.wiremock.common.Strings.isNotEmpty;
import static com.github.tomakehurst.wiremock.common.ssl.KeyStoreSettings.NO_STORE;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.github.tomakehurst.wiremock.common.NetworkAddressRules;
import com.github.tomakehurst.wiremock.common.ProxySettings;
import com.github.tomakehurst.wiremock.common.ssl.KeyStoreSettings;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.http.NetworkAddressRulesAdheringDnsResolver;
import com.github.tomakehurst.wiremock.http.client.ApacheBackedHttpClient;
import com.github.tomakehurst.wiremock.http.client.HttpClient;
import com.github.tomakehurst.wiremock.http.client.HttpClientFactory;
import com.github.tomakehurst.wiremock.http.ssl.*;
import java.security.*;
import java.util.Enumeration;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.DefaultAuthenticationStrategy;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.ManagedHttpClientConnectionFactory;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.socket.LayeredConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.config.CharCodingConfig;
import org.apache.hc.core5.util.TextUtils;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

/**
 * A custom HTTP client factory implementation for WireMock-based fake services.
 *
 * <p>This factory creates HTTP clients specifically configured for intercepting and recording HTTP
 * requests during integration testing. It extends the standard WireMock HTTP client factory with
 * customizations required for Databricks JDBC driver testing, such as custom user agent settings.
 * Majority of the implementation is inspired from {@link
 * com.github.tomakehurst.wiremock.http.client.ApacheHttpClientFactory}.
 *
 * <p>This factory is utilized by the {@link FakeServiceExtension} to establish HTTP clients for
 * various Databricks service types such as SQL Execution API, Cloud Fetch API, and JWT token
 * endpoints during integration testing.
 *
 * <p>The clients created by this factory support both recording mode (capturing real API
 * interactions) and replay mode (using previously captured responses) as defined by the {@link
 * FakeServiceExtension.FakeServiceMode}.
 *
 * @see FakeServiceExtension
 * @see AbstractFakeServiceIntegrationTests
 * @see com.github.tomakehurst.wiremock.http.client.HttpClientFactory
 */
public class FakeServiceHttpClientFactory implements HttpClientFactory {
  private final String userAgent;

  public FakeServiceHttpClientFactory(String userAgent) {
    this.userAgent = userAgent;
  }

  @Override
  public HttpClient buildHttpClient(
      Options options,
      boolean trustAllCertificates,
      List<String> trustedHosts,
      boolean useSystemProperties) {
    final CloseableHttpClient apacheClient =
        createClient(
            options.getMaxHttpClientConnections(),
            options.proxyTimeout(),
            options.proxyVia(),
            options.httpsSettings().trustStore(),
            trustAllCertificates,
            trustedHosts,
            useSystemProperties,
            options.getProxyTargetRules(),
            true);

    return new ApacheBackedHttpClient(apacheClient);
  }

  public CloseableHttpClient createClient(
      int maxConnections,
      int timeoutMilliseconds,
      ProxySettings proxySettings,
      KeyStoreSettings trustStoreSettings,
      boolean trustAllCertificates,
      final List<String> trustedHosts,
      boolean useSystemProperties,
      NetworkAddressRules networkAddressRules,
      boolean disableConnectionReuse) {

    NetworkAddressRulesAdheringDnsResolver dnsResolver =
        new NetworkAddressRulesAdheringDnsResolver(networkAddressRules);

    HttpClientBuilder builder =
        HttpClientBuilder.create()
            .disableAuthCaching()
            .disableAutomaticRetries()
            .disableCookieManagement()
            .disableRedirectHandling()
            .disableContentCompression()
            .setConnectionManager(
                PoolingHttpClientConnectionManagerBuilder.create()
                    .setDnsResolver(dnsResolver)
                    .setMaxConnPerRoute(maxConnections)
                    .setMaxConnTotal(maxConnections)
                    .setValidateAfterInactivity(TimeValue.ofSeconds(5)) // TODO Verify duration
                    .setConnectionFactory(
                        new ManagedHttpClientConnectionFactory(
                            null, CharCodingConfig.custom().setCharset(UTF_8).build(), null))
                    .build())
            .setDefaultRequestConfig(
                RequestConfig.custom()
                    .setResponseTimeout(Timeout.ofMilliseconds(timeoutMilliseconds))
                    .build());

    if (disableConnectionReuse) {
      builder
          .setConnectionReuseStrategy((request, response, context) -> false)
          .setKeepAliveStrategy((response, context) -> TimeValue.ZERO_MILLISECONDS);
    }

    if (useSystemProperties) {
      builder.useSystemProperties();
    }

    if (proxySettings != NO_PROXY) {
      HttpHost proxyHost = new HttpHost(proxySettings.host(), proxySettings.port());
      builder.setProxy(proxyHost);
      if (isNotEmpty(proxySettings.getUsername()) && isNotEmpty(proxySettings.getPassword())) {
        builder.setProxyAuthenticationStrategy(new DefaultAuthenticationStrategy()); // TODO Verify
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            new AuthScope(proxySettings.host(), proxySettings.port()),
            new UsernamePasswordCredentials(
                proxySettings.getUsername(), proxySettings.getPassword().toCharArray()));
        builder.setDefaultCredentialsProvider(credentialsProvider);
      }
    }

    final SSLContext sslContext =
        buildSslContext(trustStoreSettings, trustAllCertificates, trustedHosts);
    LayeredConnectionSocketFactory sslSocketFactory = buildSslConnectionSocketFactory(sslContext);
    PoolingHttpClientConnectionManager connectionManager =
        PoolingHttpClientConnectionManagerBuilder.create()
            .setSSLSocketFactory(sslSocketFactory)
            .setDnsResolver(dnsResolver)
            .build();
    builder.setConnectionManager(connectionManager);

    // Set the user agent
    builder.setUserAgent(userAgent);

    return builder.build();
  }

  private static SSLContext buildSslContext(
      KeyStoreSettings trustStoreSettings,
      boolean trustAllCertificates,
      List<String> trustedHosts) {
    if (trustStoreSettings != NO_STORE) {
      return buildSSLContextWithTrustStore(trustStoreSettings, trustAllCertificates, trustedHosts);
    } else if (trustAllCertificates) {
      return buildAllowAnythingSSLContext();
    } else {
      try {
        return SSLContextBuilder.create()
            .loadTrustMaterial(new TrustSpecificHostsStrategy(trustedHosts))
            .build();
      } catch (NoSuchAlgorithmException | KeyManagementException e) {
        return throwUnchecked(e, null);
      }
    }
  }

  private static SSLContext buildSSLContextWithTrustStore(
      KeyStoreSettings trustStoreSettings,
      boolean trustSelfSignedCertificates,
      List<String> trustedHosts) {
    try {
      KeyStore trustStore = trustStoreSettings.loadStore();
      SSLContextBuilder sslContextBuilder =
          SSLContextBuilder.create()
              .loadKeyMaterial(trustStore, trustStoreSettings.password().toCharArray());
      if (trustSelfSignedCertificates) {
        sslContextBuilder.loadTrustMaterial(new TrustSelfSignedStrategy());
      } else if (containsCertificate(trustStore)) {
        sslContextBuilder.loadTrustMaterial(
            trustStore, new TrustSpecificHostsStrategy(trustedHosts));
      } else {
        sslContextBuilder.loadTrustMaterial(new TrustSpecificHostsStrategy(trustedHosts));
      }
      return sslContextBuilder.build();
    } catch (Exception e) {
      return throwUnchecked(e, SSLContext.class);
    }
  }

  private static boolean containsCertificate(KeyStore trustStore) throws KeyStoreException {
    Enumeration<String> aliases = trustStore.aliases();
    while (aliases.hasMoreElements()) {
      String alias = aliases.nextElement();
      try {
        if (trustStore.getEntry(alias, null) instanceof KeyStore.TrustedCertificateEntry) {
          return true;
        }
      } catch (NoSuchAlgorithmException | UnrecoverableEntryException e) {
        // ignore
      }
    }
    return false;
  }

  private static SSLContext buildAllowAnythingSSLContext() {
    try {
      return SSLContextBuilder.create().loadTrustMaterial(new TrustEverythingStrategy()).build();
    } catch (Exception e) {
      return throwUnchecked(e, null);
    }
  }

  private static LayeredConnectionSocketFactory buildSslConnectionSocketFactory(
      final SSLContext sslContext) {
    final String[] supportedProtocols = split(System.getProperty("https.protocols"));
    final String[] supportedCipherSuites = split(System.getProperty("https.cipherSuites"));

    return new SSLConnectionSocketFactory(
        new HostVerifyingSSLSocketFactory(sslContext.getSocketFactory()),
        supportedProtocols,
        supportedCipherSuites,
        new NoopHostnameVerifier() // using Java's hostname verification
        );
  }

  private static String[] split(final String s) {
    if (TextUtils.isBlank(s)) {
      return null;
    }
    return s.split(" *, *");
  }
}
