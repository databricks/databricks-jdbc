package com.databricks.jdbc.dbclient.impl.http;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.dbclient.impl.common.RevocationTestFixture;
import com.databricks.jdbc.dbclient.impl.common.RevocationTestFixture.Responder;
import java.util.Properties;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.junit.jupiter.api.Test;

/**
 * Thrift path: {@link DatabricksHttpClient} builds its connection manager through the same shared
 * path as the SEA client. The JSSE failure carries the PKIX reason only in its message, so this
 * test matches the revoked message.
 */
class DatabricksHttpClientRevocationTest {

  private static final String STRICT = "CheckCertRevocation=1;AcceptUndeterminedRevocation=0";
  private static final String SOFT = "CheckCertRevocation=1;AcceptUndeterminedRevocation=1";

  /** Returns true when the request succeeds, false when it is rejected as revoked. */
  private static boolean acceptedThroughThrift(RevocationTestFixture fixture, String settings)
      throws Exception {
    DatabricksHttpClient client =
        new DatabricksHttpClient(
            DatabricksConnectionContext.parse(fixture.jdbcUrl(settings), new Properties()),
            HttpClientType.COMMON);
    try (client;
        CloseableHttpResponse response = client.execute(new HttpGet(fixture.httpsUrl()))) {
      return response.getStatusLine().getStatusCode() == 200;
    } catch (Exception e) {
      assertTrue(String.valueOf(e.getMessage()).contains("has been revoked"), String.valueOf(e));
      return false;
    }
  }

  @Test
  void strict_crlRevoked_rejected() throws Exception {
    try (RevocationTestFixture fixture =
        new RevocationTestFixture(Responder.REVOKED, Responder.UNAVAILABLE, true)) {
      fixture.installDefaultTrustStore(true);
      assertFalse(acceptedThroughThrift(fixture, STRICT));
    }
  }

  @Test
  void soft_ocspRevokedOnly_rejected() throws Exception {
    try (RevocationTestFixture fixture =
        new RevocationTestFixture(Responder.GOOD, Responder.REVOKED, true)) {
      fixture.installDefaultTrustStore(true);
      assertFalse(acceptedThroughThrift(fixture, SOFT));
    }
  }
}
