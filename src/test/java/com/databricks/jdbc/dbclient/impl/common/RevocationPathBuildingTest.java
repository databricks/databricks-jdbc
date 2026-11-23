package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.dbclient.impl.common.RevocationChainFixture.Endpoint;
import com.databricks.jdbc.dbclient.impl.common.RevocationTestFixture.Responder;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.junit.jupiter.api.Test;

/**
 * Chain validation over presented chains that carry certificates outside the path: an unrelated
 * root, a cross-signed root, or an out-of-order order of the intermediates. The JDK's own path
 * builder selects the path from the presented certificates, so extra ones are ignored, and a chain
 * with a missing intermediate is still refused. Both modes use the trust manager the driver
 * installs.
 */
class RevocationPathBuildingTest {

  /** The trust manager the driver installs for these trust anchors and mode. */
  private static X509TrustManager trustManager(Set<TrustAnchor> anchors, boolean soft)
      throws Exception {
    TrustManagerFactory factory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    factory.init(ConfiguratorUtils.buildTrustManagerParameters(anchors, true, soft));
    return (X509TrustManager) factory.getTrustManagers()[0];
  }

  /** Checks the chain in soft mode against the fixture's root, the only trust anchor. */
  private static void checkServer(RevocationChainFixture fixture, X509Certificate[] chain)
      throws Exception {
    checkServer(Set.of(new TrustAnchor(fixture.certificates()[3], null)), chain, true);
  }

  private static void checkServer(Set<TrustAnchor> anchors, X509Certificate[] chain, boolean soft)
      throws Exception {
    trustManager(anchors, soft).checkServerTrusted(chain, "RSA");
  }

  /** Leaf, intermediate one, then intermediate two: the order a server may send. */
  private static X509Certificate[] outOfOrder(X509Certificate[] presented) {
    return new X509Certificate[] {presented[0], presented[2], presented[1]};
  }

  private static X509Certificate[] with(X509Certificate[] chain, X509Certificate extra) {
    X509Certificate[] result = Arrays.copyOf(chain, chain.length + 1);
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
      X500Name issuer, X500Name subject, KeyPair signer, PublicKey subjectKey) throws Exception {
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
      checkServer(fixture, with(fixture.certificates(), unrelatedRoot()));
      assertEquals(1, fixture.requestCount(Endpoint.CRL_LEAF));
      assertEquals(1, fixture.requestCount(Endpoint.CRL_INTERMEDIATE_TWO));
    }
  }

  @Test
  void validChainWithAppendedCrossSignedRootIsAccepted() throws Exception {
    try (RevocationChainFixture fixture = new RevocationChainFixture(Map.of())) {
      checkServer(
          fixture, with(fixture.certificates(), crossSignedRoot(fixture.certificates()[3])));
      assertEquals(1, fixture.requestCount(Endpoint.CRL_LEAF));
      assertEquals(1, fixture.requestCount(Endpoint.CRL_INTERMEDIATE_TWO));
    }
  }

  @Test
  void outOfOrderChainWithExtraCertificateIsAccepted() throws Exception {
    try (RevocationChainFixture fixture = new RevocationChainFixture(Map.of())) {
      checkServer(fixture, with(outOfOrder(fixture.certificates()), unrelatedRoot()));
      assertEquals(1, fixture.requestCount(Endpoint.CRL_LEAF));
      assertEquals(1, fixture.requestCount(Endpoint.CRL_INTERMEDIATE_TWO));
    }
  }

  @Test
  void revokedIntermediateOutOfOrderWithExtraCertificateIsRejected() throws Exception {
    try (RevocationChainFixture fixture =
        new RevocationChainFixture(Map.of(Endpoint.CRL_INTERMEDIATE_TWO, Responder.REVOKED))) {
      X509Certificate[] chain = with(outOfOrder(fixture.certificates()), unrelatedRoot());
      assertThrows(CertificateException.class, () -> checkServer(fixture, chain));
      assertTrue(fixture.requestCount(Endpoint.CRL_INTERMEDIATE_TWO) >= 1);
    }
  }

  @Test
  void revokedLeafOutOfOrderIsRejected() throws Exception {
    try (RevocationChainFixture fixture =
        new RevocationChainFixture(Map.of(Endpoint.CRL_LEAF, Responder.REVOKED))) {
      X509Certificate[] chain = outOfOrder(fixture.certificates());
      assertThrows(CertificateException.class, () -> checkServer(fixture, chain));
      assertTrue(fixture.requestCount(Endpoint.CRL_LEAF) >= 1);
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

  /**
   * Two trust anchors share the chain root's subject name but not its key, and the intermediate has
   * no AKID, so the path builder can try the wrong anchor first. The intermediate is revoked by
   * both CRL and OCSP. Each run uses fresh keys, which changes the order the anchors are tried in,
   * and each run must reject in both modes.
   */
  @Test
  void sameNameAnchorsRejectRevokedIntermediateOnEveryRun() throws Exception {
    Map<Endpoint, Responder> revoked =
        Map.of(
            Endpoint.CRL_INTERMEDIATE_TWO, Responder.REVOKED,
            Endpoint.OCSP_INTERMEDIATE_TWO, Responder.REVOKED);
    for (int run = 0; run < 12; run++) {
      try (RevocationChainFixture fixture = new RevocationChainFixture(revoked)) {
        Set<TrustAnchor> anchors =
            Set.of(
                new TrustAnchor(fixture.certificates()[3], null),
                new TrustAnchor(fixture.sameNameRoot(), null));
        for (boolean soft : new boolean[] {false, true}) {
          assertThrows(
              CertificateException.class,
              () -> checkServer(anchors, fixture.certificates(), soft),
              "run "
                  + run
                  + (soft ? " soft" : " strict")
                  + " must reject the revoked intermediate");
        }
      }
    }
  }
}
