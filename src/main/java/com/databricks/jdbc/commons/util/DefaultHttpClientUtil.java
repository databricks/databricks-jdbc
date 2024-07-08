package com.databricks.jdbc.commons.util;

import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;

public class DefaultHttpClientUtil {
  public static DefaultHttpClient getDefaultHttpClient() throws Exception {
    DefaultHttpClient httpClient = new DefaultHttpClient();
    SSLContext ssl_ctx = SSLContext.getInstance("TLS");
    TrustManager[] certs =
        new TrustManager[] {
          new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
              return null;
            }

            public void checkClientTrusted(X509Certificate[] certs, String t) {}

            public void checkServerTrusted(X509Certificate[] certs, String t) {}
          }
        };
    ssl_ctx.init(null, certs, new SecureRandom());
    SSLSocketFactory ssf =
        new SSLSocketFactory(ssl_ctx, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
    ClientConnectionManager ccm = httpClient.getConnectionManager();
    SchemeRegistry sr = ccm.getSchemeRegistry();
    sr.register(new Scheme("https", 443, ssf));
    return new DefaultHttpClient(ccm, httpClient.getParams());
  }
}
