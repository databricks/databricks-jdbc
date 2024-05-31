package com.databricks.jdbc.integration.e2e;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.client.impl.sdk.DatabricksUCVolumeClient;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class UCVolumeTests {

  private DatabricksUCVolumeClient client;
  private Connection con;

  @BeforeEach
  void setUp() throws SQLException {
    DriverManager.registerDriver(new com.databricks.jdbc.driver.DatabricksDriver());
    DriverManager.drivers().forEach(driver -> System.out.println(driver.getClass()));
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=https;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;UID=token;";
    con = DriverManager.getConnection(jdbcUrl, "agnipratim.nag@databricks.com", "xx");
    System.out.println("Connection established......");

    client = new DatabricksUCVolumeClient(con);
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (con != null) {
      con.close();
    }
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPrefixExists")
  void testPrefixExists(
      String catalog,
      String schema,
      String volume,
      String prefix,
      boolean caseSensitive,
      boolean expected)
      throws Exception {
    boolean result = client.prefixExists(catalog, schema, volume, prefix, caseSensitive);
    assertEquals(expected, result);
  }

  private static Stream<Arguments> provideParametersForPrefixExists() {
    return Stream.of(
        Arguments.of("samikshya_hackathon", "agnipratim_test", "abc_volume1", "abc", true, true),
        Arguments.of("samikshya_hackathon", "agnipratim_test", "abc_volume1", "xyz", false, false),
        Arguments.of("samikshya_hackathon", "agnipratim_test", "abc_volume1", "dEf", false, true),
        Arguments.of("samikshya_hackathon", "agnipratim_test", "abc_volume1", "#!", true, true),
        Arguments.of("samikshya_hackathon", "agnipratim_test", "abc_volume1", "aBc", true, true));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForObjectExistsCaseSensitivity")
  void testObjectExistsCaseSensitivity(
      String catalog,
      String schema,
      String volume,
      String objectName,
      boolean caseSensitive,
      boolean expected)
      throws Exception {
    boolean result = client.objectExists(catalog, schema, volume, objectName, caseSensitive);
    assertEquals(expected, result);
  }

  private static Stream<Arguments> provideParametersForObjectExistsCaseSensitivity() {
    return Stream.of(
        Arguments.of(
            "samikshya_hackathon", "agnipratim_test", "abc_volume1", "abc_file1.csv", true, false),
        Arguments.of(
            "samikshya_hackathon", "agnipratim_test", "abc_volume1", "aBc_file1.csv", true, true),
        Arguments.of(
            "samikshya_hackathon", "agnipratim_test", "abc_volume1", "abc_file1.csv", false, true));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForObjectExistsVolumeReferencing")
  void testObjectExistsVolumeReferencing(
      String catalog,
      String schema,
      String volume,
      String objectName,
      boolean caseSensitive,
      boolean expected)
      throws Exception {
    boolean result = client.objectExists(catalog, schema, volume, objectName, caseSensitive);
    assertEquals(expected, result);
  }

  private static Stream<Arguments> provideParametersForObjectExistsVolumeReferencing() {
    return Stream.of(
        Arguments.of(
            "samikshya_hackathon", "agnipratim_test", "abc_volume1", "abc_file3.csv", true, true),
        Arguments.of(
            "samikshya_hackathon", "agnipratim_test", "abc_volume2", "abc_file4.csv", true, true),
        Arguments.of(
            "samikshya_hackathon", "agnipratim_test", "abc_volume1", "abc_file2.csv", true, true),
        Arguments.of(
            "samikshya_hackathon", "agnipratim_test", "abc_volume2", "abc_file2.csv", true, true),
        Arguments.of(
            "samikshya_hackathon", "agnipratim_test", "abc_volume1", "abc_file4.csv", true, false),
        Arguments.of(
            "samikshya_hackathon", "agnipratim_test", "abc_volume2", "abc_file3.csv", true, false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForObjectExistsSpecialCharacters")
  void testObjectExistsSpecialCharacters(
      String catalog,
      String schema,
      String volume,
      String objectName,
      boolean caseSensitive,
      boolean expected)
      throws Exception {
    boolean result = client.objectExists(catalog, schema, volume, objectName, caseSensitive);
    assertEquals(expected, result);
  }

  private static Stream<Arguments> provideParametersForObjectExistsSpecialCharacters() {
    return Stream.of(
        Arguments.of(
            "samikshya_hackathon", "agnipratim_test", "abc_volume1", "@!aBc_file1.csv", true, true),
        Arguments.of(
            "samikshya_hackathon", "agnipratim_test", "abc_volume1", "@aBc_file1.csv", true, false),
        Arguments.of(
            "samikshya_hackathon", "agnipratim_test", "abc_volume1", "#!#_file3.csv", true, true),
        Arguments.of(
            "samikshya_hackathon", "agnipratim_test", "abc_volume1", "#_file3.csv", true, false));
  }
}
