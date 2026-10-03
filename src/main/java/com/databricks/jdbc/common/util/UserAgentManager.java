package com.databricks.jdbc.common.util;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.UserAgent;
import com.google.common.annotations.VisibleForTesting;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class UserAgentManager {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(UserAgentManager.class);
  private static final String SDK_USER_AGENT = "databricks-sdk-java";
  private static final String JDBC_HTTP_USER_AGENT = "databricks-jdbc-http";
  private static final String DEFAULT_USER_AGENT = "DatabricksJDBCDriverOSS";
  private static final String CLIENT_USER_AGENT_PREFIX = "Java";
  public static final String USER_AGENT_SEA_CLIENT = "SQLExecHttpClient";
  public static final String USER_AGENT_THRIFT_CLIENT = "THttpClient";
  private static final String SEA_CLIENT_SEGMENT =
      CLIENT_USER_AGENT_PREFIX + "/" + USER_AGENT_SEA_CLIENT;
  private static final String VERSION_FILLER = "version";
  private static final String AGENT_KEY = "agent";

  /**
   * Bounds SDK user-agent growth when an application passes a distinct {@code UserAgentEntry} per
   * connection. Client-type entries are not counted.
   */
  @VisibleForTesting static final int MAX_CUSTOMER_USER_AGENT_ENTRIES = 64;

  // UserAgent.withOtherInfo appends on every call and UserAgent.asString formats the whole list on
  // every request, so each entry is registered with the SDK once per JVM.
  private static final Set<String> registeredOtherInfo = new HashSet<>();
  private static int registeredCustomerEntries = 0;

  /**
   * Parse custom user agent string into name and version components.
   *
   * @param customerUserAgent The custom user agent string (may be URL encoded)
   * @return String array [name, version] or null if parsing fails
   */
  private static String[] parseCustomerUserAgent(String customerUserAgent) {
    try {
      String decodedUA = URLDecoder.decode(customerUserAgent, StandardCharsets.UTF_8);
      int i = decodedUA.indexOf('/');
      String customerName = (i < 0) ? decodedUA : decodedUA.substring(0, i);
      String customerVersion = (i < 0) ? VERSION_FILLER : decodedUA.substring(i + 1);
      return new String[] {customerName, customerVersion};
    } catch (Exception e) {
      LOGGER.debug("Failed to parse customer userAgent entry {}, Error {}", customerUserAgent, e);
      return null;
    }
  }

  /** Returns a validated name/version segment, or null if the entry cannot be registered. */
  public static String customerUserAgentSegment(String customerUserAgent) {
    if (customerUserAgent == null) {
      return null;
    }
    String[] parsed = parseCustomerUserAgent(customerUserAgent);
    if (parsed == null) {
      return null;
    }
    try {
      String version = UserAgent.sanitize(parsed[1]);
      UserAgent.matchAlphanum(parsed[0]);
      UserAgent.matchAlphanumOrSemVer(version);
      return parsed[0] + "/" + version;
    } catch (IllegalArgumentException e) {
      LOGGER.debug("Failed to set customer userAgent entry {}, Error {}", customerUserAgent, e);
      return null;
    }
  }

  /**
   * Set the user agent for the Databricks JDBC driver.
   *
   * @param connectionContext The connection context.
   */
  public static void setUserAgent(IDatabricksConnectionContext connectionContext) {
    // Set the base product
    UserAgent.withProduct(DEFAULT_USER_AGENT, DriverUtil.getDriverVersion());

    // Set client info (this may trigger getClientType which fetches feature flags)
    registerOtherInfo(CLIENT_USER_AGENT_PREFIX, connectionContext.getClientUserAgent(), false);

    String customerSegment = customerUserAgentSegment(connectionContext.getCustomerUserAgent());
    if (customerSegment != null) {
      int slash = customerSegment.indexOf('/');
      registerOtherInfo(
          customerSegment.substring(0, slash), customerSegment.substring(slash + 1), true);
    }
  }

  private static synchronized void registerOtherInfo(
      String key, String value, boolean isCustomerEntry) {
    String entry = key + "/" + value;
    if (registeredOtherInfo.contains(entry)) {
      return;
    }
    if (isCustomerEntry && registeredCustomerEntries >= MAX_CUSTOMER_USER_AGENT_ENTRIES) {
      LOGGER.debug(
          "Not adding userAgent entry {}: limit of {} distinct entries reached",
          entry,
          MAX_CUSTOMER_USER_AGENT_ENTRIES);
      return;
    }
    UserAgent.withOtherInfo(key, value);
    registeredOtherInfo.add(entry);
    if (isCustomerEntry) {
      registeredCustomerEntries++;
    }
  }

  /** Forgets registered entries; the SDK keeps them, so re-registration only adds duplicates. */
  @VisibleForTesting
  static synchronized void resetRegisteredOtherInfo() {
    registeredOtherInfo.clear();
    registeredCustomerEntries = 0;
  }

  /**
   * Build user agent string for connector service requests (without client type to avoid circular
   * dependency). This is used specifically for feature flags requests since client type is not yet
   * determined.
   *
   * @param connectionContext The connection context.
   * @return User agent string with format: "DatabricksJDBCDriverOSS/version databricks-jdbc-http
   *     jvm/version os/name [CustomApp/version]"
   */
  public static String buildUserAgentForConnectorService(
      IDatabricksConnectionContext connectionContext) {
    StringBuilder userAgent = new StringBuilder();

    // Base product: DatabricksJDBCDriverOSS/version
    userAgent.append(DEFAULT_USER_AGENT).append("/").append(DriverUtil.getDriverVersion());

    // JDBC HTTP identifier
    userAgent.append(" ").append(JDBC_HTTP_USER_AGENT);

    // JVM version
    userAgent
        .append(" jvm/")
        .append(System.getProperty("java.version", "unknown").replace(" ", "_"));

    // OS name
    userAgent.append(" os/").append(System.getProperty("os.name", "unknown").replace(" ", "_"));

    String customerSegment = customerUserAgentSegment(connectionContext.getCustomerUserAgent());
    if (customerSegment != null) {
      userAgent.append(" ").append(customerSegment);
    }

    // Detect AI coding agent and append to user agent
    AgentDetector.detect()
        .ifPresent(product -> userAgent.append(" ").append(AGENT_KEY).append("/").append(product));

    return userAgent.toString();
  }

  /** Gets the user agent string for Databricks Driver HTTP Client. */
  public static String getUserAgentString() {
    String sdkUserAgent = UserAgent.asString();
    // Split the string into parts
    String[] parts = sdkUserAgent.split("\\s+");
    // User Agent is in format:
    // product/product-version databricks-sdk-java/sdk-version jvm/jvm-version other-info
    // Remove the SDK part from user agent
    StringBuilder mergedString = new StringBuilder();
    for (int i = 0; i < parts.length; i++) {
      if (parts[i].startsWith(SDK_USER_AGENT)) {
        mergedString.append(JDBC_HTTP_USER_AGENT);
      } else {
        mergedString.append(parts[i]);
      }
      if (i != parts.length - 1) {
        mergedString.append(" "); // Add space between parts
      }
    }
    return mergedString.toString();
  }

  /** Places a validated customer segment before the SEA marker, immediately after os. */
  public static String orderSeaUserAgent(String sdkUserAgent, String customerSegment) {
    if (sdkUserAgent == null || customerSegment == null) {
      return sdkUserAgent;
    }

    List<String> segments = new ArrayList<>(Arrays.asList(sdkUserAgent.split("\\s+")));
    int osIndex = -1;
    for (int i = 0; i < segments.size(); i++) {
      if (segments.get(i).startsWith("os/")) {
        osIndex = i;
        break;
      }
    }
    if (osIndex < 0) {
      return sdkUserAgent;
    }
    List<String> extraInfo = segments.subList(osIndex + 1, segments.size());
    if (SEA_CLIENT_SEGMENT.equals(customerSegment)) {
      return sdkUserAgent;
    }

    extraInfo.removeIf(
        segment -> segment.equals(customerSegment) || segment.equals(SEA_CLIENT_SEGMENT));
    extraInfo.add(0, SEA_CLIENT_SEGMENT);
    extraInfo.add(0, customerSegment);
    return String.join(" ", segments);
  }
}
