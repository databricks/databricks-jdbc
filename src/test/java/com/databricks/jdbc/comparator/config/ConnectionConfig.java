package com.databricks.jdbc.comparator.config;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Declarative registry of connection parameter configurations for the JDBC comparator.
 *
 * <p>Each constant defines a named set of extra JDBC connection parameters and the test suites it
 * applies to. The same parameters are appended to both Thrift and SEA URLs.
 *
 * <p>To add a new configuration, add a new enum constant — no other code changes needed.
 */
public enum ConnectionConfig {
  DEFAULT_PARAMS(
      "Default params",
      Map.of(),
      null,
      allExcept(
          TestSuite.COMPLEX_TYPES,
          TestSuite.GEOSPATIAL,
          TestSuite.VOLUME_OPERATIONS,
          TestSuite.NEGATIVE_VOLUME)),

  COMPRESSION_DISABLED(
      "Compression disabled",
      Map.of("EnableQueryResultLZ4Compression", "0"),
      null,
      EnumSet.of(TestSuite.STATEMENT_SELECT)),

  COMPLEX_TYPES_DISABLED(
      "ComplexTypes disabled",
      Map.of("EnableComplexDatatypeSupport", "0"),
      null,
      EnumSet.of(TestSuite.COMPLEX_TYPES)),

  COMPLEX_TYPES_ENABLED(
      "ComplexTypes enabled",
      Map.of("EnableComplexDatatypeSupport", "1"),
      null,
      EnumSet.of(TestSuite.COMPLEX_TYPES)),

  GEOSPATIAL_DISABLED(
      "Geospatial disabled",
      Map.of("EnableComplexDatatypeSupport", "1", "EnableGeoSpatialSupport", "0"),
      null,
      EnumSet.of(TestSuite.GEOSPATIAL)),

  GEOSPATIAL_ENABLED(
      "Geospatial enabled",
      Map.of("EnableComplexDatatypeSupport", "1", "EnableGeoSpatialSupport", "1"),
      null,
      EnumSet.of(TestSuite.GEOSPATIAL)),

  USE_QUERY_FOR_METADATA(
      "UseQueryForMetadata",
      Map.of("UseQueryForMetadata", "1"),
      null,
      EnumSet.of(TestSuite.DATABASE_METADATA)),

  DIRECT_RESULTS_DISABLED(
      "Direct results disabled",
      Map.of("EnableDirectResults", "0"),
      null,
      EnumSet.of(TestSuite.STATEMENT_SELECT)),

  VOLUME_OPERATIONS(
      "Volume operations",
      Map.of("VolumeOperationAllowedLocalPaths", "/tmp"),
      null,
      EnumSet.of(TestSuite.VOLUME_OPERATIONS, TestSuite.NEGATIVE_VOLUME)),

  PRO_WAREHOUSE(
      "Pro warehouse",
      Map.of("EnableComplexDatatypeSupport", "1", "EnableGeoSpatialSupport", "1"),
      System.getProperty("PRO_WAREHOUSE_ID") != null
          ? "/sql/1.0/warehouses/" + System.getProperty("PRO_WAREHOUSE_ID")
          : null,
      EnumSet.of(TestSuite.STATEMENT_SELECT, TestSuite.COMPLEX_TYPES, TestSuite.GEOSPATIAL)),
  ;

  private static final String CONFIG_FILTER_PROPERTY = "CONNECTION_CONFIG";

  private final String displayName;
  private final Map<String, String> params;
  private final String httpPathOverride;
  private final EnumSet<TestSuite> applicableSuites;

  ConnectionConfig(
      String displayName,
      Map<String, String> params,
      String httpPathOverride,
      EnumSet<TestSuite> applicableSuites) {
    this.displayName = displayName;
    this.params = params;
    this.httpPathOverride = httpPathOverride;
    this.applicableSuites = applicableSuites;
  }

  public String getDisplayName() {
    return displayName;
  }

  public boolean appliesTo(TestSuite suite) {
    return applicableSuites.contains(suite);
  }

  public EnumSet<TestSuite> getApplicableSuites() {
    return applicableSuites;
  }

  /**
   * Returns all configs, filtered by the optional {@code CONNECTION_CONFIG} system property.
   *
   * <p>Usage: {@code -DCONNECTION_CONFIG=DEFAULT,USE_QUERY_FOR_METADATA}
   */
  public static List<ConnectionConfig> activeConfigs() {
    String filter = System.getProperty(CONFIG_FILTER_PROPERTY);
    Set<String> allowed =
        (filter == null || filter.isEmpty())
            ? null
            : new HashSet<>(Arrays.asList(filter.split(",")));

    return Arrays.stream(values())
        .filter(c -> allowed == null || allowed.contains(c.name()))
        .filter(c -> c != PRO_WAREHOUSE || c.httpPathOverride != null)
        .collect(Collectors.toList());
  }

  /** Builds a JDBC URL by appending extra params to the base URL. */
  public String buildUrl(String baseUrl) {
    String url = baseUrl;
    if (httpPathOverride != null) {
      url = url.replaceFirst("httpPath=[^;]*", "httpPath=" + httpPathOverride);
    }
    StringBuilder sb = new StringBuilder(url);
    for (Map.Entry<String, String> entry : params.entrySet()) {
      sb.append(';').append(entry.getKey()).append('=').append(entry.getValue());
    }
    return sb.toString();
  }

  private static EnumSet<TestSuite> allExcept(TestSuite... excluded) {
    return EnumSet.complementOf(EnumSet.copyOf(Arrays.asList(excluded)));
  }
}
