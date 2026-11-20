package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.dbclient.impl.common.RevocationChainFixture.Endpoint;
import com.databricks.jdbc.dbclient.impl.common.RevocationTestFixture.Responder;
import java.math.BigInteger;
import java.net.Socket;
import java.security.KeyPair;
import java.security.cert.CertificateException;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.junit.jupiter.api.Test;

/**
 * Soft-mode fallback over a presented chain that carries certificates outside the path: an
 * unrelated root, a cross-signed root, or a certificate the path does not need. The validated path
 * is built from the presented certificates, so extra ones are ignored, and a chain with a missing
 * intermediate is still refused.
 */
class RevocationPathBuildingTest {

  /** Stands in for a JSSE pass: it accepts, or fails with the given exception. */
  private static final class StubPass extends X509ExtendedTrustManager {
    private final CertificateException failure;

    StubPass(CertificateException failure) {
      this.failure = failure;
    }

    private void check() throws CertificateException {
      if (failure != null) {
        throw failure;
      }
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
        throws CertificateException {
      check();
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
        throws CertificateException {
      check();
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      check();
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
        throws CertificateException {
      check();
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
        throws CertificateException {
      check();
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      check();
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  /** Runs the soft-fail server check, with the first pass failing without an index. */
  private static void checkServer(RevocationChainFixture fixture, X509Certificate[] chain)
      throws CertificateException {
    Set<TrustAnchor> anchors = Set.of(new TrustAnchor(fixture.certificates()[3], null));
    TrustManager[] managers =
        ConfiguratorUtils.softFailTrustManagers(
            new StubPass(new CertificateException("pass stopped without an index")),
            new StubPass(null),
            anchors,
            new TrustManager[0]);
    ((X509TrustManager) managers[0]).checkServerTrusted(chain, "RSA");
  }

  /** Leaf, intermediate one, then intermediate two: the order a server may send. */
  private static X509Certificate[] outOfOrder(X509Certificate[] presented) {
    return new X509Certificate[] {presented[0], presented[2], presented[1]};
  }

  private static X509Certificate[] with(X509Certificate[] chain, X509Certificate extra) {
    X509Certificate[] result = java.util.Arrays.copyOf(chain, chain.length + 1);
    result[chain.length] = extra;
    return result;
  }

  /** A self-signed root that anchors nothing in the trust store. */
  private static X509Certificate unrelatedRoot() throws Exception {
    KeyPair keys = RevocationTestFixture.newKeyPair();
    X500Name name = new X500Name("CN=Unrelated Test Root");
    return issue(name, name, keys, keys.getPublic());
  }

  /**
   * The trust store's root name and key, issued by an unrelated CA: a cross-signed copy of the
   * anchor that the presented chain carries alongside the real root.
   */
  private static X509Certificate crossSignedRoot(X509Certificate root) throws Exception {
    KeyPair otherCa = RevocationTestFixture.newKeyPair();
    return issue(
        new X500Name("CN=Other Test CA"),
        new X500Name("CN=Chain Test Root"),
        otherCa,
        root.getPublicKey());
  }

  private static X509Certificate issue(
      X500Name issuer, X500Name subject, KeyPair signer, java.security.PublicKey subjectKey)
      throws Exception {
    Date now = new Date();
    JcaX509v3CertificateBuilder builder =
        new JcaX509v3CertificateBuilder(
            issuer,
            BigInteger.valueOf(System.nanoTime() & Long.MAX_VALUE),
            new Date(now.getTime() - 60_000L),
            new Date(now.getTime() + 86_400_000L),
            subject,
            subjectKey);
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
    return new JcaX509CertificateConverter()
        .getCertificate(builder.build(RevocationTestFixture.contentSigner(signer)));
  }

  @Test
  void validChainWithUnrelatedExtraCertificateIsAccepted() throws Exception {
    try (RevocationChainFixture fixture = new RevocationChainFixture(Map.of())) {
      X509Certificate[] chain = with(fixture.certificates(), unrelatedRoot());
      checkServer(fixture, chain);
      assertEquals(1, fixture.requestCount(Endpoint.OCSP_LEAF));
      assertEquals(1, fixture.requestCount(Endpoint.OCSP_INTERMEDIATE_TWO));
    }
  }

  @Test
  void validChainWithAppendedCrossSignedRootIsAccepted() throws Exception {
    try (RevocationChainFixture fixture = new RevocationChainFixture(Map.of())) {
      X509Certificate[] chain =
          with(fixture.certificates(), crossSignedRoot(fixture.certificates()[3]));
      checkServer(fixture, chain);
      assertEquals(1, fixture.requestCount(Endpoint.OCSP_LEAF));
      assertEquals(1, fixture.requestCount(Endpoint.OCSP_INTERMEDIATE_TWO));
    }
  }

  @Test
  void outOfOrderChainWithExtraCertificateIsAccepted() throws Exception {
    try (RevocationChainFixture fixture = new RevocationChainFixture(Map.of())) {
      checkServer(fixture, with(outOfOrder(fixture.certificates()), unrelatedRoot()));
      assertEquals(1, fixture.requestCount(Endpoint.OCSP_LEAF));
      assertEquals(1, fixture.requestCount(Endpoint.OCSP_INTERMEDIATE_TWO));
    }
  }

  @Test
  void revokedIntermediateOutOfOrderWithExtraCertificateIsRejected() throws Exception {
    try (RevocationChainFixture fixture =
        new RevocationChainFixture(Map.of(Endpoint.OCSP_INTERMEDIATE_TWO, Responder.REVOKED))) {
      X509Certificate[] chain = with(outOfOrder(fixture.certificates()), unrelatedRoot());
      assertThrows(CertificateException.class, () -> checkServer(fixture, chain));
      assertTrue(fixture.requestCount(Endpoint.OCSP_INTERMEDIATE_TWO) >= 1);
    }
  }

  @Test
  void extraCertificateDoesNotStandInForMissingIntermediate() throws Exception {
    try (RevocationChainFixture fixture = new RevocationChainFixture(Map.of())) {
      X509Certificate[] presented = fixture.certificates();
      // Leaf and intermediate two, with intermediate one absent and only an unrelated root
      // presented: no path reaches the anchor, so the chain is refused.
      X509Certificate[] chain = new X509Certificate[] {presented[0], presented[1], unrelatedRoot()};
      assertThrows(CertificateException.class, () -> checkServer(fixture, chain));
    }
  }
}
