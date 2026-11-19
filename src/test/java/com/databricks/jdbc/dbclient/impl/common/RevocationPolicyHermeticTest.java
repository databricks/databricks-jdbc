package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.common.RevocationTestFixture.Responder;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateRevokedException;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Revocation policy on the default trust store, end to end over a generated CA, a loopback CRL and
 * OCSP responder, and a loopback HTTPS endpoint. The shared connection manager covers every case.
 * The SEA client configurator and the Thrift client each cover a focused subset.
 */
class RevocationPolicyHermeticTest {

  static final String STRICT = "CheckCertRevocation=1;AcceptUndeterminedRevocation=0";
  static final String SOFT = "CheckCertRevocation=1;AcceptUndeterminedRevocation=1";

  enum Expect {
    ACCEPTED,
    REVOKED,
    REJECTED
  }

  /** Outcome of one HTTPS request: accepted, or the TLS failure. */
  static final class Outcome {
    final boolean accepted;
    final Throwable failure;

    Outcome(boolean accepted, Throwable failure) {
      this.accepted = accepted;
      this.failure = failure;
    }

    boolean revoked() {
      for (Throwable t = failure; t != null; t = t.getCause()) {
        if (t instanceof CertificateRevokedException
            || (t instanceof CertPathValidatorException
                && ((CertPathValidatorException) t).getReason()
                    == CertPathValidatorException.BasicReason.REVOKED)) {
          return true;
        }
      }
      return false;
    }
  }

  static Stream<Arguments> cases() {
    return Stream.of(
        // name, CRL, OCSP, OCSP access location, settings, revocation checked, expected
        Arguments.of(
            "omitted: revoked accepted",
            Responder.REVOKED,
            Responder.REVOKED,
            true,
            "",
            false,
            Expect.ACCEPTED),
        Arguments.of(
            "strict: good accepted",
            Responder.GOOD,
            Responder.GOOD,
            true,
            STRICT,
            true,
            Expect.ACCEPTED),
        Arguments.of(
            "strict: CRL revoked, OCSP unavailable, rejected",
            Responder.REVOKED,
            Responder.UNAVAILABLE,
            true,
            STRICT,
            true,
            Expect.REVOKED),
        Arguments.of(
            "strict: OCSP revoked only, rejected",
            Responder.GOOD,
            Responder.REVOKED,
            true,
            STRICT,
            true,
            Expect.REVOKED),
        Arguments.of(
            "strict: unavailable, rejected",
            Responder.UNAVAILABLE,
            Responder.UNAVAILABLE,
            true,
            STRICT,
            true,
            Expect.REJECTED),
        Arguments.of(
            "soft: unavailable, accepted",
            Responder.UNAVAILABLE,
            Responder.UNAVAILABLE,
            true,
            SOFT,
            true,
            Expect.ACCEPTED),
        Arguments.of(
            "soft: OCSP revoked only, rejected",
            Responder.GOOD,
            Responder.REVOKED,
            true,
            SOFT,
            true,
            Expect.REVOKED),
        Arguments.of(
            "soft: CRL revoked, OCSP unavailable, rejected",
            Responder.REVOKED,
            Responder.UNAVAILABLE,
            true,
            SOFT,
            true,
            Expect.REVOKED),
        Arguments.of(
            "soft: good accepted",
            Responder.GOOD,
            Responder.GOOD,
            true,
            SOFT,
            true,
            Expect.ACCEPTED),
        Arguments.of(
            "CRL-only leaf, strict: CRL unavailable, rejected",
            Responder.UNAVAILABLE,
            Responder.UNAVAILABLE,
            false,
            STRICT,
            true,
            Expect.REJECTED),
        Arguments.of(
            "CRL-only leaf, soft: CRL unavailable, accepted",
            Responder.UNAVAILABLE,
            Responder.UNAVAILABLE,
            false,
            SOFT,
            true,
            Expect.ACCEPTED),
        Arguments.of(
            "CRL-only leaf, soft: CRL revoked, rejected",
            Responder.REVOKED,
            Responder.UNAVAILABLE,
            false,
            SOFT,
            true,
            Expect.REVOKED),
        Arguments.of(
            "CRL-only leaf, strict: CRL good, accepted",
            Responder.GOOD,
            Responder.UNAVAILABLE,
            false,
            STRICT,
            true,
            Expect.ACCEPTED),
        Arguments.of(
            "CRL-only leaf, soft: CRL good, accepted",
            Responder.GOOD,
            Responder.UNAVAILABLE,
            false,
            SOFT,
            true,
            Expect.ACCEPTED),
        Arguments.of(
            "CheckCertRevocation=0: revoked accepted, no requests",
            Responder.REVOKED,
            Responder.REVOKED,
            true,
            "CheckCertRevocation=0",
            false,
            Expect.ACCEPTED),
        // A definitive OCSP GOOD answers the check, so a conflicting CRL entry is not consulted.
        // Strict and soft must agree on this input.
        Arguments.of(
            "strict: OCSP good overrides CRL revoked, accepted",
            Responder.REVOKED,
            Responder.GOOD,
            true,
            STRICT,
            true,
            Expect.ACCEPTED),
        Arguments.of(
            "soft: OCSP good overrides CRL revoked, accepted",
            Responder.REVOKED,
            Responder.GOOD,
            true,
            SOFT,
            true,
            Expect.ACCEPTED));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void sharedManagerAppliesRevocationPolicy(
      String name,
      Responder crl,
      Responder ocsp,
      boolean withOcspAccessLocation,
      String settings,
      boolean revocationChecked,
      Expect expected)
      throws Exception {
    try (RevocationTestFixture fixture =
        new RevocationTestFixture(crl, ocsp, withOcspAccessLocation)) {
      fixture.installDefaultTrustStore(true);
      Outcome outcome =
          fetch(ConfiguratorUtils.getBaseConnectionManager(context(fixture, settings)), fixture);
      assertOutcome(outcome, expected);
      assertEquals(revocationChecked, fixture.revocationRequestCount() > 0);
    }
  }

  @Test
  void seaClient_strictOcspRevokedRejected() throws Exception {
    try (RevocationTestFixture fixture =
        new RevocationTestFixture(Responder.GOOD, Responder.REVOKED, true)) {
      fixture.installDefaultTrustStore(true);
      assertOutcome(fetchViaSea(fixture, STRICT), Expect.REVOKED);
    }
  }

  @Test
  void seaClient_softUnavailableAccepted() throws Exception {
    try (RevocationTestFixture fixture =
        new RevocationTestFixture(Responder.UNAVAILABLE, Responder.UNAVAILABLE, true)) {
      fixture.installDefaultTrustStore(true);
      assertOutcome(fetchViaSea(fixture, SOFT), Expect.ACCEPTED);
    }
  }

  @Test
  void checkRevocationOff_chainValidationStillEnforced() throws Exception {
    try (RevocationTestFixture fixture =
        new RevocationTestFixture(Responder.GOOD, Responder.GOOD, true)) {
      // The trust store holds only an unrelated CA, so the server chain anchors nowhere. The
      // handshake must fail even with revocation off, which proves chain validation still runs.
      fixture.installDefaultTrustStoreWithUnrelatedCa();
      Outcome outcome =
          fetch(
              ConfiguratorUtils.getBaseConnectionManager(context(fixture, "CheckCertRevocation=0")),
              fixture);
      assertFalse(outcome.accepted, "an unanchored chain must be refused with revocation off");
      assertTrue(
          sslFailure(outcome.failure), "expected a TLS chain failure, got: " + outcome.failure);
    }
  }

  @Test
  void soft_unanchoredChainRejected() throws Exception {
    try (RevocationTestFixture fixture =
        new RevocationTestFixture(Responder.GOOD, Responder.GOOD, true)) {
      // Soft mode builds SoftFailTrustManager here, so this covers the path that rethrows the
      // first-pass failure when neither pass determines a revoked status.
      fixture.installDefaultTrustStoreWithUnrelatedCa();
      Outcome outcome =
          fetch(ConfiguratorUtils.getBaseConnectionManager(context(fixture, SOFT)), fixture);
      assertFalse(outcome.accepted, "an unanchored chain must be refused in soft mode");
      assertTrue(
          sslFailure(outcome.failure), "expected a TLS chain failure, got: " + outcome.failure);
    }
  }

  private static boolean sslFailure(Throwable failure) {
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (t instanceof javax.net.ssl.SSLException) {
        return true;
      }
    }
    return false;
  }

  private static void assertOutcome(Outcome outcome, Expect expected) {
    switch (expected) {
      case ACCEPTED:
        assertTrue(outcome.accepted, String.valueOf(outcome.failure));
        break;
      case REVOKED:
        assertFalse(outcome.accepted);
        assertTrue(outcome.revoked(), String.valueOf(outcome.failure));
        break;
      case REJECTED:
        assertFalse(outcome.accepted, "expected rejection");
        break;
    }
  }

  private static IDatabricksConnectionContext context(
      RevocationTestFixture fixture, String settings) throws Exception {
    return DatabricksConnectionContext.parse(fixture.jdbcUrl(settings), new Properties());
  }

  private static Outcome fetchViaSea(RevocationTestFixture fixture, String settings)
      throws Exception {
    try (ClientConfigurator configurator = new ClientConfigurator(context(fixture, settings))) {
      return fetch(configurator.getSdkConnectionManager(), fixture);
    }
  }

  private static Outcome fetch(
      PoolingHttpClientConnectionManager manager, RevocationTestFixture fixture) {
    try (org.apache.http.impl.client.CloseableHttpClient client =
        HttpClients.custom().setConnectionManager(manager).build()) {
      try (CloseableHttpResponse response = client.execute(new HttpGet(fixture.httpsUrl()))) {
        return new Outcome(response.getStatusLine().getStatusCode() == 200, null);
      }
    } catch (Exception e) {
      return new Outcome(false, e);
    }
  }
}
