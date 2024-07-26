package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.TELEMETRY_LOG_LEVEL;

import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.commons.util.LoggingUtil;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;

public class DatabricksUsageMetrics {
  private static final String URL =
      "https://aa87314c1e33d4c1f91a919f8cf9c4ba-387609431.us-west-2.elb.amazonaws.com:443/api/2.0/oss-sql-driver-telemetry/usageMetrics";
  private static String workspaceId = null;
  private static DatabricksHttpClient telemetryClient;

  private static final String WORKSPACE_ID = "workspace_id";
  private static final String JVM_NAME = "jvm_name";
  private static final String JVM_SPEC_VERSION = "jvm_spec_version";
  private static final String JVM_IMPL_VERSION = "jvm_impl_version";
  private static final String JVM_VENDOR = "jvm_vendor";
  private static final String OS_NAME = "os_name";
  private static final String OS_VERSION = "os_version";
  private static final String OS_ARCH = "os_arch";
  private static final String LOCALE_NAME = "locale_name";
  private static final String CHARSET_ENCODING = "charset_encoding";

  private static void initialize(IDatabricksConnectionContext context)
      throws DatabricksSQLException {
    workspaceId = context.getComputeResource().getWorkspaceId();
    telemetryClient = DatabricksHttpClient.getInstance(context);
  }

  private static HttpPost getRequest(
      String jvmName,
      String jvmSpecVersion,
      String jvmImplVersion,
      String jvmVendor,
      String osName,
      String osVersion,
      String osArch,
      String localeName,
      String charsetEncoding)
      throws Exception {
    URIBuilder uriBuilder = new URIBuilder(URL);
    HttpPost request = new HttpPost(uriBuilder.build());
    request.setHeader(WORKSPACE_ID, workspaceId);
    request.setHeader(JVM_NAME, jvmName);
    request.setHeader(JVM_SPEC_VERSION, jvmSpecVersion);
    request.setHeader(JVM_IMPL_VERSION, jvmImplVersion);
    request.setHeader(JVM_VENDOR, jvmVendor);
    request.setHeader(OS_NAME, osName);
    request.setHeader(OS_VERSION, osVersion);
    request.setHeader(OS_ARCH, osArch);
    request.setHeader(LOCALE_NAME, localeName);
    request.setHeader(CHARSET_ENCODING, charsetEncoding);
    return request;
  }

  public static void exportUsageMetrics(
      IDatabricksConnectionContext context,
      String jvmName,
      String jvmSpecVersion,
      String jvmImplVersion,
      String jvmVendor,
      String osName,
      String osVersion,
      String osArch,
      String localeName,
      String charsetEncoding) {
    if (!context.enableTelemetry()) {
      return;
    }
    try {
      initialize(context);
      HttpUriRequest request =
          getRequest(
              jvmName,
              jvmSpecVersion,
              jvmImplVersion,
              jvmVendor,
              osName,
              osVersion,
              osArch,
              localeName,
              charsetEncoding);
      CloseableHttpResponse response = telemetryClient.executeWithoutCertVerification(request);

      if (response == null) {
        LoggingUtil.log(TELEMETRY_LOG_LEVEL, "Response is null for usage metrics export.");
      } else if (response.getStatusLine().getStatusCode() != 200) {
        LoggingUtil.log(
            TELEMETRY_LOG_LEVEL,
            "Response code for usage metrics export: "
                + response.getStatusLine().getStatusCode()
                + " Response: "
                + response.getEntity().toString());
      } else {
        LoggingUtil.log(TELEMETRY_LOG_LEVEL, EntityUtils.toString(response.getEntity()));
        response.close();
      }
    } catch (Exception e) {
      LoggingUtil.log(TELEMETRY_LOG_LEVEL, e.getMessage());
    }
  }
}
