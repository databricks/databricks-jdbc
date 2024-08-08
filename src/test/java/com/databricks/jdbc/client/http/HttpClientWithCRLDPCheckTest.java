package com.databricks.jdbc.client.http;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.*;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.*;
import java.math.BigInteger;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HttpClientWithCRLDPCheckTest {

  private MockWebServer mockWebServer;
  private X509Certificate certificateWithCRLDP;
  private X509Certificate certificateWithoutCRLDP;

  @BeforeEach
  public void setUp() throws Exception {
    Security.addProvider(new BouncyCastleProvider());

    // Generate certificates
    certificateWithCRLDP = generateSelfSignedCertificate(true);
    certificateWithoutCRLDP = generateSelfSignedCertificate(false);

    // Set up a mock server with the certificate
    mockWebServer = new MockWebServer();
    mockWebServer.useHttps(createSslSocketFactory(certificateWithCRLDP), false);
    mockWebServer.start();

    // Enqueue a mock response
    mockWebServer.enqueue(new MockResponse().setBody("Hello, world!"));
  }

  @After
  public void tearDown() throws Exception {
    mockWebServer.shutdown();
  }

  @Test
  public void testHttpGetAndCheckCertificate() throws Exception {
    // Create an SSLContext with our existing CustomX509TrustManager
    TrustManagerFactory trustManagerFactory =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init((KeyStore) null);
    TrustManager[] trustAllCerts = new TrustManager[]{new CustomX509TrustManager()};

    KeyManagerFactory keyManagerFactory =
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    // Initialize SSLContext
    SSLContext sslContext = SSLContextBuilder.create()
            .loadTrustMaterial(null, (chain, authType) -> true)
            .build();

    SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(
            sslContext, NoopHostnameVerifier.INSTANCE);

    CloseableHttpClient httpClient = HttpClientBuilder.create()
            .setSSLSocketFactory(sslSocketFactory)
            .build();

    HttpGet httpGet = new HttpGet(mockWebServer.url("/").toString());
    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
      assertNotNull(response);
      assertEquals(200, response.getStatusLine().getStatusCode());

      // Validate that the certificate was used
      // Note: HttpClient does not provide direct access to the server's certificate
      // during a request, so you need to validate this indirectly
    }
  }

  private X509Certificate generateSelfSignedCertificate(boolean withCRLDP) throws Exception {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    KeyPair keyPair = keyPairGenerator.generateKeyPair();

    X509v3CertificateBuilder certificateBuilder = new JcaX509v3CertificateBuilder(
            new X500Name("CN=Test"),
            BigInteger.valueOf(System.currentTimeMillis()),
            new Date(System.currentTimeMillis() - 1000 * 60 * 60 * 24),
            new Date(System.currentTimeMillis() + 1000 * 60 * 60 * 24),
            new X500Name("CN=Test"),
            keyPair.getPublic()
    );

    if (withCRLDP) {
      // Add CRL Distribution Points (CRLDP) extension
      certificateBuilder.addExtension(
              new ASN1ObjectIdentifier("2.5.29.31"), false,
              new CRLDistPoint(new DistributionPoint[]{
                      new DistributionPoint(
                              new DistributionPointName(
                                      new GeneralNames(new GeneralName(GeneralName.uniformResourceIdentifier, "http://crl.example.com/crl.crl"))
                              ),
                              null,
                              null
                      )
              })
      );
    }

    JcaContentSignerBuilder contentSignerBuilder = new JcaContentSignerBuilder("SHA256withRSA");
    ContentSigner contentSigner = contentSignerBuilder.build(keyPair.getPrivate());
    X509Certificate certificate = new JcaX509CertificateConverter()
            .setProvider("BC")
            .getCertificate(certificateBuilder.build(contentSigner));

    return certificate;
  }

  private SSLSocketFactory createSslSocketFactory(X509Certificate certificate) throws Exception {
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null); // Load an empty key store
    keyStore.setCertificateEntry("cert", certificate);

    KeyStore trustStore = KeyStore.getInstance("PKCS12");
    trustStore.load(null, null); // Load an empty trust store
    trustStore.setCertificateEntry("cert", certificate);

    TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);

    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, null);

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
    return sslContext.getSocketFactory();
  }
//
//  static class TrustManagerFactory extends javax.net.ssl.TrustManagerFactorySpi {
//    @Override
//    protected void engineInit(KeyStore keyStore) {
//      // No-op
//    }
//
//    @Override
//    protected void engineInit(ManagerFactoryParameters managerFactoryParameters) {
//      // No-op
//    }
//
//    @Override
//    protected TrustManager[] engineGetTrustManagers() {
//      return new TrustManager[]{new CustomX509TrustManager()};
//    }
//  }

  static class CustomX509TrustManager implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      // No-op
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      // Implement your CRLDP logic here
      for (X509Certificate cert : chain) {
        Set<String> critExtOids = cert.getCriticalExtensionOIDs();
        if (critExtOids != null && critExtOids.contains("2.5.29.31")) {
          // CRLDP is present, perform default check
          System.out.println("Certificate with CRLDP: " + cert);
          return;
        }
      }
      // CRLDP not present, skip revocation check
      System.out.println("Certificate without CRLDP: " + chain[0]);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }
}
