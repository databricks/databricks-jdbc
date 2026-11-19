package com.databricks.jdbc.dbclient.impl.common;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AuthorityInformationAccess;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.CRLDistPoint;
import org.bouncycastle.asn1.x509.CRLReason;
import org.bouncycastle.asn1.x509.DistributionPoint;
import org.bouncycastle.asn1.x509.DistributionPointName;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.asn1.x509.X509ObjectIdentifiers;
import org.bouncycastle.cert.X509CRLHolder;
import org.bouncycastle.cert.X509v2CRLBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.cert.ocsp.BasicOCSPResp;
import org.bouncycastle.cert.ocsp.BasicOCSPRespBuilder;
import org.bouncycastle.cert.ocsp.CertificateID;
import org.bouncycastle.cert.ocsp.CertificateStatus;
import org.bouncycastle.cert.ocsp.OCSPReq;
import org.bouncycastle.cert.ocsp.OCSPResp;
import org.bouncycastle.cert.ocsp.OCSPRespBuilder;
import org.bouncycastle.cert.ocsp.Req;
import org.bouncycastle.cert.ocsp.RevokedStatus;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DigestCalculator;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.operator.jcajce.JcaDigestCalculatorProviderBuilder;

/**
 * Hermetic PKI for revocation tests: a generated CA, a server certificate whose CRL distribution
 * point (and optionally OCSP access location) points at a loopback responder, and a loopback HTTPS
 * endpoint that presents it. Keys stay in memory. The only file written is a temporary trust store,
 * which is deleted on close.
 */
public final class RevocationTestFixture implements AutoCloseable {

  /** What the CRL or OCSP endpoint returns for the server certificate. */
  public enum Responder {
    GOOD,
    REVOKED,
    /** HTTP 503, so the status cannot be determined. */
    UNAVAILABLE
  }

  private static final String PASSWORD = "changeit";
  private static final String SERVER_HOST = "127.0.0.1";
  private static final String[] TRUST_STORE_PROPERTIES = {
    "javax.net.ssl.trustStore", "javax.net.ssl.trustStoreType", "javax.net.ssl.trustStorePassword"
  };

  private final String id = UUID.randomUUID().toString();
  private final KeyPair caKeys;
  private final KeyPair leafKeys;
  private final X509Certificate caCertificate;
  private final BigInteger leafSerial = BigInteger.valueOf(System.nanoTime() & Long.MAX_VALUE);
  private final Responder crl;
  private final Responder ocsp;
  private final AtomicInteger revocationRequests = new AtomicInteger();
  private final HttpServer revocationServer;
  private final HttpsServer tlsServer;
  private final ExecutorService revocationExecutor = Executors.newCachedThreadPool();
  private final ExecutorService tlsExecutor = Executors.newCachedThreadPool();
  private final String[] savedProperties = new String[TRUST_STORE_PROPERTIES.length];
  private Path trustStoreFile;

  /**
   * Starts the servers and builds the PKI.
   *
   * @param crl what the CRL endpoint serves for the server certificate
   * @param ocsp what the OCSP endpoint serves for the server certificate
   * @param withOcspAccessLocation whether the server certificate has an OCSP access location. When
   *     false, its revocation information is the CRL alone.
   */
  public RevocationTestFixture(Responder crl, Responder ocsp, boolean withOcspAccessLocation)
      throws Exception {
    this.crl = crl;
    this.ocsp = ocsp;
    this.caKeys = newKeyPair();
    this.leafKeys = newKeyPair();

    // The revocation endpoint is bound first, because the server certificate names its URLs.
    revocationServer = HttpServer.create(new InetSocketAddress(SERVER_HOST, 0), 0);
    String base =
        "http://" + SERVER_HOST + ":" + revocationServer.getAddress().getPort() + "/" + id;
    caCertificate = buildCaCertificate();
    X509Certificate leafCertificate =
        buildLeafCertificate(base + "/crl.crl", withOcspAccessLocation ? base + "/ocsp" : null);
    revocationServer.createContext("/" + id + "/crl.crl", this::serveCrl);
    revocationServer.createContext("/" + id + "/ocsp", this::serveOcsp);
    revocationServer.setExecutor(revocationExecutor);
    revocationServer.start();

    tlsServer = HttpsServer.create(new InetSocketAddress(SERVER_HOST, 0), 0);
    tlsServer.setHttpsConfigurator(new HttpsConfigurator(buildSslContext(leafCertificate)));
    tlsServer.createContext(
        "/ping",
        exchange -> {
          byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    tlsServer.setExecutor(tlsExecutor);
    tlsServer.start();
  }

  /** Returns the URL of the HTTPS endpoint. */
  public String httpsUrl() {
    return "https://" + SERVER_HOST + ":" + tlsServer.getAddress().getPort() + "/ping";
  }

  /** Returns a JDBC URL that points at this endpoint, with the given extra settings. */
  public String jdbcUrl(String extraSettings) {
    return "jdbc:databricks://"
        + SERVER_HOST
        + ":"
        + tlsServer.getAddress().getPort()
        + ";ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/0000000000000000;PWD=dapi-test-token"
        + (extraSettings.isEmpty() ? "" : ";" + extraSettings);
  }

  /** Returns the number of CRL and OCSP requests served so far. */
  public int revocationRequestCount() {
    return revocationRequests.get();
  }

  /**
   * Points the JVM default trust store (javax.net.ssl.trustStore) at a file that holds this CA, or
   * at an empty store when trustThisCa is false. The default trust store path reads this property.
   */
  public void installDefaultTrustStore(boolean trustThisCa) throws Exception {
    installTrustStore(trustThisCa ? caCertificate : null);
  }

  /**
   * Points the JVM default trust store at a store that holds an unrelated self-signed CA, so the
   * server certificate chains to no trust anchor and the handshake must fail.
   */
  public void installDefaultTrustStoreWithUnrelatedCa() throws Exception {
    installTrustStore(unrelatedCaCertificate());
  }

  private void installTrustStore(X509Certificate anchor) throws Exception {
    KeyStore store = KeyStore.getInstance("PKCS12");
    store.load(null, null);
    if (anchor != null) {
      store.setCertificateEntry("revocation-test-ca", anchor);
    }
    trustStoreFile = Files.createTempFile("revocation-test-trust-", ".p12");
    try (OutputStream out = Files.newOutputStream(trustStoreFile)) {
      store.store(out, PASSWORD.toCharArray());
    }
    for (int i = 0; i < TRUST_STORE_PROPERTIES.length; i++) {
      savedProperties[i] = System.getProperty(TRUST_STORE_PROPERTIES[i]);
    }
    System.setProperty(TRUST_STORE_PROPERTIES[0], trustStoreFile.toString());
    System.setProperty(TRUST_STORE_PROPERTIES[1], "PKCS12");
    System.setProperty(TRUST_STORE_PROPERTIES[2], PASSWORD);
  }

  @Override
  public void close() throws IOException {
    tlsServer.stop(0);
    revocationServer.stop(0);
    tlsExecutor.shutdownNow();
    revocationExecutor.shutdownNow();
    if (trustStoreFile != null) {
      for (int i = 0; i < TRUST_STORE_PROPERTIES.length; i++) {
        if (savedProperties[i] == null) {
          System.clearProperty(TRUST_STORE_PROPERTIES[i]);
        } else {
          System.setProperty(TRUST_STORE_PROPERTIES[i], savedProperties[i]);
        }
      }
      Files.deleteIfExists(trustStoreFile);
      trustStoreFile = null;
    }
  }

  private void serveCrl(HttpExchange exchange) throws IOException {
    revocationRequests.incrementAndGet();
    if (crl == Responder.UNAVAILABLE) {
      respond(exchange, 503, new byte[0]);
      return;
    }
    try {
      Date now = new Date();
      X509v2CRLBuilder builder = new X509v2CRLBuilder(caName(), now);
      builder.setNextUpdate(new Date(now.getTime() + 86_400_000L));
      if (crl == Responder.REVOKED) {
        builder.addCRLEntry(leafSerial, now, CRLReason.keyCompromise);
      }
      X509CRLHolder holder = builder.build(contentSigner());
      respond(exchange, 200, holder.getEncoded());
    } catch (Exception e) {
      respond(exchange, 500, new byte[0]);
    }
  }

  private void serveOcsp(HttpExchange exchange) throws IOException {
    revocationRequests.incrementAndGet();
    if (ocsp == Responder.UNAVAILABLE) {
      respond(exchange, 503, new byte[0]);
      return;
    }
    try {
      OCSPReq request = new OCSPReq(ocspRequestBytes(exchange));
      DigestCalculator sha1 =
          new JcaDigestCalculatorProviderBuilder().build().get(CertificateID.HASH_SHA1);
      BasicOCSPRespBuilder builder =
          new BasicOCSPRespBuilder(
              SubjectPublicKeyInfo.getInstance(caKeys.getPublic().getEncoded()), sha1);
      for (Req req : request.getRequestList()) {
        CertificateStatus status =
            ocsp == Responder.REVOKED
                ? new RevokedStatus(new Date(), CRLReason.keyCompromise)
                : CertificateStatus.GOOD;
        builder.addResponse(req.getCertID(), status);
      }
      BasicOCSPResp basic = builder.build(contentSigner(), null, new Date());
      OCSPResp response = new OCSPRespBuilder().build(OCSPRespBuilder.SUCCESSFUL, basic);
      respond(exchange, 200, response.getEncoded());
    } catch (Exception e) {
      respond(exchange, 500, new byte[0]);
    }
  }

  /**
   * Returns the DER OCSP request. JDK 17+ sends it as a GET with the base64 request URL-encoded in
   * the path, while JDK 11 sends a POST body. Both forms are accepted so the responder works on
   * every JDK under test.
   */
  private static byte[] ocspRequestBytes(HttpExchange exchange) throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      return exchange.getRequestBody().readAllBytes();
    }
    String path = exchange.getRequestURI().getRawPath();
    int start = path.indexOf("/ocsp/");
    if (start < 0) {
      throw new IOException("OCSP GET request has no encoded request in path: " + path);
    }
    String encoded = path.substring(start + "/ocsp/".length());
    return Base64.getDecoder().decode(URLDecoder.decode(encoded, StandardCharsets.UTF_8.name()));
  }

  private X509Certificate buildCaCertificate() throws Exception {
    Date now = new Date();
    JcaX509v3CertificateBuilder builder =
        new JcaX509v3CertificateBuilder(
            caName(),
            BigInteger.valueOf(System.nanoTime() & 0xFFFFFFL).add(BigInteger.ONE),
            new Date(now.getTime() - 60_000L),
            new Date(now.getTime() + 86_400_000L),
            caName(),
            caKeys.getPublic());
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
    builder.addExtension(
        Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
    return new JcaX509CertificateConverter().getCertificate(builder.build(contentSigner()));
  }

  private X509Certificate buildLeafCertificate(String crlUrl, String ocspUrl) throws Exception {
    Date now = new Date();
    JcaX509v3CertificateBuilder builder =
        new JcaX509v3CertificateBuilder(
            caName(),
            leafSerial,
            new Date(now.getTime() - 60_000L),
            new Date(now.getTime() + 86_400_000L),
            new X500Name("CN=" + SERVER_HOST),
            leafKeys.getPublic());
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
    builder.addExtension(
        Extension.subjectAlternativeName,
        false,
        new GeneralNames(new GeneralName(GeneralName.iPAddress, SERVER_HOST)));
    builder.addExtension(
        Extension.extendedKeyUsage, false, new ExtendedKeyUsage(KeyPurposeId.id_kp_serverAuth));
    builder.addExtension(
        Extension.cRLDistributionPoints,
        false,
        new CRLDistPoint(
            new DistributionPoint[] {
              new DistributionPoint(
                  new DistributionPointName(
                      new GeneralNames(
                          new GeneralName(GeneralName.uniformResourceIdentifier, crlUrl))),
                  null,
                  null)
            }));
    if (ocspUrl != null) {
      builder.addExtension(
          Extension.authorityInfoAccess,
          false,
          new AuthorityInformationAccess(
              X509ObjectIdentifiers.ocspAccessMethod,
              new GeneralName(GeneralName.uniformResourceIdentifier, ocspUrl)));
    }
    return new JcaX509CertificateConverter().getCertificate(builder.build(contentSigner()));
  }

  private SSLContext buildSslContext(X509Certificate leafCertificate) throws Exception {
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);
    keyStore.setKeyEntry(
        "server",
        leafKeys.getPrivate(),
        PASSWORD.toCharArray(),
        new Certificate[] {leafCertificate, caCertificate});
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, PASSWORD.toCharArray());
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(kmf.getKeyManagers(), null, null);
    return sslContext;
  }

  private X509Certificate unrelatedCaCertificate() throws Exception {
    KeyPair keys = newKeyPair();
    X500Name name = new X500Name("CN=Unrelated Revocation Test CA");
    Date now = new Date();
    JcaX509v3CertificateBuilder builder =
        new JcaX509v3CertificateBuilder(
            name,
            BigInteger.ONE,
            new Date(now.getTime() - 60_000L),
            new Date(now.getTime() + 86_400_000L),
            name,
            keys.getPublic());
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
    builder.addExtension(
        Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
    ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(keys.getPrivate());
    return new JcaX509CertificateConverter().getCertificate(builder.build(signer));
  }

  private X500Name caName() {
    return new X500Name("CN=Revocation Test CA " + id);
  }

  private ContentSigner contentSigner() throws Exception {
    return new JcaContentSignerBuilder("SHA256withRSA").build(caKeys.getPrivate());
  }

  private static KeyPair newKeyPair() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    return generator.generateKeyPair();
  }

  private static void respond(HttpExchange exchange, int status, byte[] body) throws IOException {
    exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(body);
    }
  }
}
