package com.databricks.jdbc.dbclient.impl.common;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
import org.bouncycastle.operator.DigestCalculator;
import org.bouncycastle.operator.jcajce.JcaDigestCalculatorProviderBuilder;

/**
 * Hermetic chain for the multi-intermediate soft-mode cases: root, then intermediate one (no OCSP
 * access location, unreachable CRL), then intermediate two, then the server certificate. Every
 * revocation endpoint counts its requests, so a test can bound the fetches a handshake makes.
 *
 * <p>Each endpoint answers with its own {@link RevocationTestFixture.Responder} behaviour and is
 * signed by the key of the certificate it describes.
 */
public final class RevocationChainFixture implements AutoCloseable {

  /** Revocation endpoints of the chain, one per distribution point or OCSP access location. */
  public enum Endpoint {
    CRL_INTERMEDIATE_TWO,
    CRL_LEAF,
    OCSP_INTERMEDIATE_TWO,
    OCSP_LEAF
  }

  private static final String PASSWORD = "changeit";
  private static final String SERVER_HOST = "127.0.0.1";
  private static final String UNREACHABLE_CRL = "http://127.0.0.1:1/unreachable.crl";
  private static final X500Name ROOT_NAME = new X500Name("CN=Chain Test Root");
  private static final X500Name INTERMEDIATE_ONE_NAME =
      new X500Name("CN=Chain Test Intermediate One");
  private static final X500Name INTERMEDIATE_TWO_NAME =
      new X500Name("CN=Chain Test Intermediate Two");
  private static final X500Name LEAF_NAME = new X500Name("CN=" + SERVER_HOST);

  /** The server certificate extended key usage. */
  private final KeyPurposeId leafPurpose;

  private final String id = UUID.randomUUID().toString();
  private final KeyPair rootKeys = RevocationTestFixture.newKeyPair();
  private final KeyPair intermediateOneKeys = RevocationTestFixture.newKeyPair();
  private final KeyPair intermediateTwoKeys = RevocationTestFixture.newKeyPair();
  private final KeyPair leafKeys = RevocationTestFixture.newKeyPair();
  private final BigInteger intermediateTwoSerial = BigInteger.valueOf(0x1002L);
  private final BigInteger leafSerial = BigInteger.valueOf(0x1003L);
  private final Date revokedAt = new Date(System.currentTimeMillis() - 3_600_000L);

  private final Map<Endpoint, RevocationTestFixture.Responder> behaviour =
      new EnumMap<>(Endpoint.class);
  private final Map<Endpoint, AtomicInteger> requests = new ConcurrentHashMap<>();
  private final HttpServer revocationServer;
  private final HttpsServer tlsServer;
  private final java.util.concurrent.ExecutorService revocationExecutor =
      Executors.newCachedThreadPool();
  private final java.util.concurrent.ExecutorService tlsExecutor = Executors.newCachedThreadPool();
  private final X509Certificate rootCertificate;
  private final X509Certificate intermediateOneCertificate;
  private final X509Certificate intermediateTwoCertificate;
  private final X509Certificate leafCertificate;
  private final String[] savedProperties = new String[3];
  private Path trustStoreFile;

  /**
   * Starts the chain. {@code behaviour} says what each endpoint answers; the chain's certificates
   * name the endpoints, so an absent entry defaults to {@code GOOD}.
   */
  public RevocationChainFixture(Map<Endpoint, RevocationTestFixture.Responder> behaviour)
      throws Exception {
    this(behaviour, false);
  }

  /** As above; {@code wrongLeafUsage} makes the server certificate clientAuth, not serverAuth. */
  public RevocationChainFixture(
      Map<Endpoint, RevocationTestFixture.Responder> behaviour, boolean wrongLeafUsage)
      throws Exception {
    this.leafPurpose =
        wrongLeafUsage ? KeyPurposeId.id_kp_clientAuth : KeyPurposeId.id_kp_serverAuth;
    this.behaviour.putAll(behaviour);
    for (Endpoint endpoint : Endpoint.values()) {
      requests.put(endpoint, new AtomicInteger());
    }
    revocationServer = HttpServer.create(new InetSocketAddress(SERVER_HOST, 0), 0);
    String base =
        "http://" + SERVER_HOST + ":" + revocationServer.getAddress().getPort() + "/" + id;
    rootCertificate = buildRoot();
    intermediateOneCertificate = buildIntermediateOne();
    intermediateTwoCertificate =
        buildIntermediateTwo(base + "/crl-intermediate-two", base + "/ocsp-intermediate-two");
    leafCertificate = buildLeaf(base + "/crl-leaf", base + "/ocsp-leaf");

    revocationServer.createContext(
        "/" + id + "/crl-intermediate-two",
        exchange -> serveCrl(exchange, Endpoint.CRL_INTERMEDIATE_TWO));
    revocationServer.createContext(
        "/" + id + "/crl-leaf", exchange -> serveCrl(exchange, Endpoint.CRL_LEAF));
    revocationServer.createContext(
        "/" + id + "/ocsp-intermediate-two",
        exchange -> serveOcsp(exchange, Endpoint.OCSP_INTERMEDIATE_TWO));
    revocationServer.createContext(
        "/" + id + "/ocsp-leaf", exchange -> serveOcsp(exchange, Endpoint.OCSP_LEAF));
    revocationServer.setExecutor(revocationExecutor);
    revocationServer.start();

    tlsServer = HttpsServer.create(new InetSocketAddress(SERVER_HOST, 0), 0);
    tlsServer.setHttpsConfigurator(new HttpsConfigurator(buildSslContext()));
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

  /** The HTTPS endpoint presenting the chain. */
  public String httpsUrl() {
    return "https://" + SERVER_HOST + ":" + tlsServer.getAddress().getPort() + "/ping";
  }

  /** A JDBC URL for the endpoint with the given extra settings. */
  public String jdbcUrl(String extraSettings) {
    return "jdbc:databricks://"
        + SERVER_HOST
        + ":"
        + tlsServer.getAddress().getPort()
        + ";ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/0000000000000000;PWD=dapi-test-token"
        + (extraSettings.isEmpty() ? "" : ";" + extraSettings);
  }

  /** Requests the endpoint has served so far. */
  public int requestCount(Endpoint endpoint) {
    return requests.get(endpoint).get();
  }

  /** The chain in presentation order: server, intermediate two, intermediate one, root. */
  public X509Certificate[] certificates() {
    return new X509Certificate[] {
      leafCertificate, intermediateTwoCertificate, intermediateOneCertificate, rootCertificate
    };
  }

  /**
   * A self-signed root with the chain root's subject name and a different key. It is not in the
   * chain, so trusting it beside the real root gives the path builder two anchors that share a
   * subject name.
   */
  public X509Certificate sameNameRoot() throws Exception {
    KeyPair keys = RevocationTestFixture.newKeyPair();
    return issue(
        ROOT_NAME,
        ROOT_NAME,
        keys,
        keys,
        BigInteger.valueOf(0x1F00L),
        true,
        null,
        null,
        null,
        null);
  }

  /** Points the JVM default trust store at a store that holds only the root. */
  public void installDefaultTrustStore() throws Exception {
    KeyStore store = KeyStore.getInstance("PKCS12");
    store.load(null, null);
    store.setCertificateEntry("chain-test-root", rootCertificate);
    trustStoreFile = Files.createTempFile("chain-revocation-trust-", ".p12");
    try (OutputStream out = Files.newOutputStream(trustStoreFile)) {
      store.store(out, PASSWORD.toCharArray());
    }
    String[] names = {
      "javax.net.ssl.trustStore", "javax.net.ssl.trustStoreType", "javax.net.ssl.trustStorePassword"
    };
    String[] values = {trustStoreFile.toString(), "PKCS12", PASSWORD};
    for (int i = 0; i < names.length; i++) {
      savedProperties[i] = System.getProperty(names[i]);
      System.setProperty(names[i], values[i]);
    }
  }

  @Override
  public void close() throws IOException {
    tlsServer.stop(0);
    revocationServer.stop(0);
    tlsExecutor.shutdownNow();
    revocationExecutor.shutdownNow();
    String[] names = {
      "javax.net.ssl.trustStore", "javax.net.ssl.trustStoreType", "javax.net.ssl.trustStorePassword"
    };
    if (trustStoreFile != null) {
      for (int i = 0; i < names.length; i++) {
        if (savedProperties[i] == null) {
          System.clearProperty(names[i]);
        } else {
          System.setProperty(names[i], savedProperties[i]);
        }
      }
      Files.deleteIfExists(trustStoreFile);
      trustStoreFile = null;
    }
  }

  private X509Certificate buildRoot() throws Exception {
    return issue(
        ROOT_NAME, ROOT_NAME, rootKeys, rootKeys, BigInteger.ONE, true, null, null, null, null);
  }

  private X509Certificate buildIntermediateOne() throws Exception {
    // No OCSP access location, and a CRL distribution point that refuses connections.
    return issue(
        ROOT_NAME,
        INTERMEDIATE_ONE_NAME,
        rootKeys,
        intermediateOneKeys,
        BigInteger.TWO,
        true,
        UNREACHABLE_CRL,
        null,
        null,
        null);
  }

  private X509Certificate buildIntermediateTwo(String crlUrl, String ocspUrl) throws Exception {
    return issue(
        INTERMEDIATE_ONE_NAME,
        INTERMEDIATE_TWO_NAME,
        intermediateOneKeys,
        intermediateTwoKeys,
        intermediateTwoSerial,
        true,
        crlUrl,
        ocspUrl,
        null,
        null);
  }

  private X509Certificate buildLeaf(String crlUrl, String ocspUrl) throws Exception {
    return issue(
        INTERMEDIATE_TWO_NAME,
        LEAF_NAME,
        intermediateTwoKeys,
        leafKeys,
        leafSerial,
        false,
        crlUrl,
        ocspUrl,
        SERVER_HOST,
        leafPurpose);
  }

  /**
   * Issues a certificate. {@code serverHost} non-null makes it a server certificate with that IP
   * SAN and the serverAuth usage.
   */
  private static X509Certificate issue(
      X500Name issuer,
      X500Name subject,
      KeyPair issuerKeys,
      KeyPair subjectKeys,
      BigInteger serial,
      boolean isCa,
      String crlUrl,
      String ocspUrl,
      String serverHost,
      KeyPurposeId serverPurpose)
      throws Exception {
    Date now = new Date();
    JcaX509v3CertificateBuilder builder =
        new JcaX509v3CertificateBuilder(
            issuer,
            serial,
            new Date(now.getTime() - 60_000L),
            new Date(now.getTime() + 86_400_000L),
            subject,
            subjectKeys.getPublic());
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(isCa));
    if (isCa) {
      builder.addExtension(
          Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
    }
    if (serverHost != null) {
      builder.addExtension(
          Extension.subjectAlternativeName,
          false,
          new GeneralNames(new GeneralName(GeneralName.iPAddress, serverHost)));
      builder.addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(serverPurpose));
    }
    if (crlUrl != null) {
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
    }
    if (ocspUrl != null) {
      builder.addExtension(
          Extension.authorityInfoAccess,
          false,
          new AuthorityInformationAccess(
              X509ObjectIdentifiers.ocspAccessMethod,
              new GeneralName(GeneralName.uniformResourceIdentifier, ocspUrl)));
    }
    return new JcaX509CertificateConverter()
        .getCertificate(builder.build(RevocationTestFixture.contentSigner(issuerKeys)));
  }

  private void serveCrl(HttpExchange exchange, Endpoint endpoint) throws IOException {
    requests.get(endpoint).incrementAndGet();
    RevocationTestFixture.Responder answer =
        behaviour.getOrDefault(endpoint, RevocationTestFixture.Responder.GOOD);
    if (answer == RevocationTestFixture.Responder.UNAVAILABLE) {
      RevocationTestFixture.respond(exchange, 503, new byte[0]);
      return;
    }
    try {
      boolean leaf = endpoint == Endpoint.CRL_LEAF;
      Date now = new Date();
      X509v2CRLBuilder builder =
          new X509v2CRLBuilder(leaf ? INTERMEDIATE_TWO_NAME : INTERMEDIATE_ONE_NAME, now);
      builder.setNextUpdate(new Date(now.getTime() + 86_400_000L));
      if (answer == RevocationTestFixture.Responder.REVOKED) {
        builder.addCRLEntry(
            leaf ? leafSerial : intermediateTwoSerial, revokedAt, CRLReason.keyCompromise);
      }
      KeyPair signer = leaf ? intermediateTwoKeys : intermediateOneKeys;
      X509CRLHolder holder = builder.build(RevocationTestFixture.contentSigner(signer));
      RevocationTestFixture.respond(exchange, 200, holder.getEncoded());
    } catch (Exception e) {
      RevocationTestFixture.respond(exchange, 500, new byte[0]);
    }
  }

  private void serveOcsp(HttpExchange exchange, Endpoint endpoint) throws IOException {
    requests.get(endpoint).incrementAndGet();
    RevocationTestFixture.Responder answer =
        behaviour.getOrDefault(endpoint, RevocationTestFixture.Responder.GOOD);
    if (answer == RevocationTestFixture.Responder.UNAVAILABLE) {
      RevocationTestFixture.respond(exchange, 503, new byte[0]);
      return;
    }
    try {
      OCSPReq request = new OCSPReq(RevocationTestFixture.ocspRequestBytes(exchange));
      DigestCalculator sha1 =
          new JcaDigestCalculatorProviderBuilder().build().get(CertificateID.HASH_SHA1);
      boolean leaf = endpoint == Endpoint.OCSP_LEAF;
      KeyPair signer = leaf ? intermediateTwoKeys : intermediateOneKeys;
      BasicOCSPRespBuilder builder =
          new BasicOCSPRespBuilder(
              SubjectPublicKeyInfo.getInstance(signer.getPublic().getEncoded()), sha1);
      for (Req req : request.getRequestList()) {
        CertificateStatus status =
            answer == RevocationTestFixture.Responder.REVOKED
                ? new RevokedStatus(revokedAt, CRLReason.keyCompromise)
                : CertificateStatus.GOOD;
        builder.addResponse(req.getCertID(), status);
      }
      BasicOCSPResp basic =
          builder.build(RevocationTestFixture.contentSigner(signer), null, new Date());
      OCSPResp response = new OCSPRespBuilder().build(OCSPRespBuilder.SUCCESSFUL, basic);
      RevocationTestFixture.respond(exchange, 200, response.getEncoded());
    } catch (Exception e) {
      RevocationTestFixture.respond(exchange, 500, new byte[0]);
    }
  }

  private SSLContext buildSslContext() throws Exception {
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);
    Certificate[] chain =
        new Certificate[] {
          leafCertificate, intermediateTwoCertificate, intermediateOneCertificate, rootCertificate
        };
    keyStore.setKeyEntry("server", leafKeys.getPrivate(), PASSWORD.toCharArray(), chain);
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, PASSWORD.toCharArray());
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(kmf.getKeyManagers(), null, null);
    return sslContext;
  }
}
