package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.dbclient.impl.common.RevocationChainFixture.Endpoint;
import com.databricks.jdbc.dbclient.impl.common.RevocationTestFixture.Responder;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.Test;

/**
 * The soft-fail per-certificate check must find each certificate's issuer by name and signature,
 * not by position, so a server that presents its chain out of order is still checked correctly.
 */
class RevocationIssuerOrderTest {

  /** Stands in for a JSSE pass: it accepts, or fails with no failure index. */
  private static final class StubPass extends X509ExtendedTrustManager {
    private final boolean accept;

    StubPass(boolean accept) {
      this.accept = accept;
    }

    private void check() throws CertificateException {
      if (!accept) {
        throw new CertificateException("pass stopped without an index");
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

  /** Runs the soft-fail server check over {@code chain}, trusting only the fixture's root. */
  private static void checkServer(RevocationChainFixture fixture, X509Certificate[] chain)
      throws CertificateException {
    Set<TrustAnchor> anchors = Set.of(new TrustAnchor(fixture.certificates()[3], null));
    TrustManager[] managers =
        ConfiguratorUtils.softFailTrustManagers(
            new StubPass(false), new StubPass(true), anchors, new TrustManager[0]);
    ((X509TrustManager) managers[0]).checkServerTrusted(chain, "RSA");
  }

  /** Leaf, the root-side intermediate, then the leaf's own issuer: the order a server may send. */
  private static X509Certificate[] outOfOrder(X509Certificate[] presented) {
    return new X509Certificate[] {presented[0], presented[2], presented[1]};
  }

  @Test
  void outOfOrderChainAcceptsGoodCertificatesAndChecksEachOnce() throws Exception {
    try (RevocationChainFixture fixture = new RevocationChainFixture(Map.of())) {
      checkServer(fixture, outOfOrder(fixture.certificates()));
      assertEquals(1, fixture.requestCount(Endpoint.OCSP_LEAF));
      assertEquals(1, fixture.requestCount(Endpoint.OCSP_INTERMEDIATE_TWO));
    }
  }

  @Test
  void outOfOrderChainRejectsRevokedLeaf() throws Exception {
    try (RevocationChainFixture fixture =
        new RevocationChainFixture(Map.of(Endpoint.OCSP_LEAF, Responder.REVOKED))) {
      assertThrows(
          CertificateException.class,
          () -> checkServer(fixture, outOfOrder(fixture.certificates())));
      assertTrue(fixture.requestCount(Endpoint.OCSP_LEAF) >= 1);
    }
  }

  @Test
  void chainWhoseIntermediateIssuerIsMissingIsRejected() throws Exception {
    try (RevocationChainFixture fixture = new RevocationChainFixture(Map.of())) {
      X509Certificate[] presented = fixture.certificates();
      // Leaf and its issuer only: intermediate two's issuer (intermediate one) is neither presented
      // nor a trust anchor, so its revocation cannot be checked and the chain fails closed.
      assertThrows(
          CertificateException.class,
          () -> checkServer(fixture, new X509Certificate[] {presented[0], presented[1]}));
    }
  }
}
