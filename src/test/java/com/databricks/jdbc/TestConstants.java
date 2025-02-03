package com.databricks.jdbc;

import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.oauth.OpenIDConnectEndpoints;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** TestConstants class contains all the constants that are used in the test classes. */
public class TestConstants {
  public static final String WAREHOUSE_ID = "warehouse_id";
  public static final String SESSION_ID = "12345678";
  public static final Warehouse WAREHOUSE_COMPUTE = new Warehouse(WAREHOUSE_ID);
  public static final IDatabricksComputeResource CLUSTER_COMPUTE =
      new AllPurposeCluster("6051921418418893", "1115-130834-ms4m0yv");
  public static final String TEST_SCHEMA = "testSchema";
  public static final String TEST_TABLE = "testTable";
  public static final Map<String, String> EMPTY_MAP = Collections.emptyMap();
  public static final String TEST_COLUMN = "testColumn";
  public static final String TEST_CATALOG = "catalog1";
  public static final String TEST_FUNCTION_PATTERN = "functionPattern";
  public static final String TEST_STRING = "test";
  public static final String TEST_USER = "testUser";
  public static final String TEST_PASSWORD = "testPassword";
  public static final String TEST_STATEMENT_ID = "testStatementId";
  public static final String UC_VOLUME_CATALOG = "uc_volume_test_catalog";
  public static final String UC_VOLUME_SCHEMA = "uc_volume_test_schema";
  public static final TSessionHandle SESSION_HANDLE =
      new TSessionHandle().setSessionId(new THandleIdentifier().setGuid(SESSION_ID.getBytes()));
  public static final ImmutableSessionInfo SESSION_INFO =
      ImmutableSessionInfo.builder()
          .sessionHandle(SESSION_HANDLE)
          .sessionId(SESSION_ID)
          .computeResource(CLUSTER_COMPUTE)
          .build();
  public static final String WAREHOUSE_JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/warehouse_id;UserAgentEntry=MyApp";
  public static final String WAREHOUSE_JDBC_URL_WITH_THRIFT =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/warehouse_id;UseThriftClient=1;";
  public static final String USER_AGENT_URL =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;UserAgentEntry=TEST/24.2.0.2712019";
  public static final String CLUSTER_JDBC_URL =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;UserAgentEntry=MyApp";
  public static final TRowSet binaryRowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.binaryVal(
                      new TBinaryColumn()
                          .setValues(
                              Collections.singletonList(
                                  ByteBuffer.wrap(TEST_STRING.getBytes()))))));
  public static final TRowSet boolRowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.boolVal(new TBoolColumn().setValues(List.of(false, true, false, true)))));
  public static final TRowSet byteRowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.byteVal(new TByteColumn().setValues(List.of((byte) 5)))));
  public static final TRowSet doubleRowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.doubleVal(
                      new TDoubleColumn().setValues(List.of(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)))));
  public static final TRowSet i16RowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.i16Val(new TI16Column().setValues(List.of((short) 1)))));
  public static final TRowSet i32RowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(TColumn.i32Val(new TI32Column().setValues(List.of(1)))));
  public static final TRowSet i64RowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.i64Val(new TI64Column().setValues(List.of(1L, 5L)))));
  public static final TRowSet stringRowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.stringVal(
                      new TStringColumn().setValues(List.of(TEST_STRING, TEST_STRING)))));
  private static final TColumnDesc TEST_COLUMN_DESCRIPTION =
      new TColumnDesc().setColumnName("testCol");
  public static final TTableSchema TEST_TABLE_SCHEMA =
      new TTableSchema().setColumns(Collections.singletonList(TEST_COLUMN_DESCRIPTION));
  public static final byte[] TEST_BYTES =
      ByteBuffer.allocate(Long.BYTES).putLong(123456789L).array();
  public static final String TEST_CLIENT_ID = "test-client-id";
  public static final String TEST_TOKEN_URL = "https://test.token.url";
  public static final String TEST_AUTH_URL = "https://test.auth.url";
  public static final String TEST_DISCOVERY_URL = "https://test.discovery.url";
  public static final String TEST_JWT_KID = "test-kid";
  public static final String TEST_SCOPE = "test-scope";
  public static final String TEST_JWT_ALGORITHM = "RS256";
  public static final String TEST_JWT_KEY_FILE = "src/test/resources/private_key.pem";
  public static final String TEST_ACCESS_TOKEN = "test-access-token";

  public static OpenIDConnectEndpoints TEST_OIDC_ENDPOINTS;

  static {
    try {
      TEST_OIDC_ENDPOINTS =
          new OpenIDConnectEndpoints("https://test.token.url", "https://test.auth.url");
    } catch (MalformedURLException e) {
      throw new DatabricksException("Can't initiate test constant for OIDC. Error: " + e);
    }
  }

  public static final String TEST_OAUTH_RESPONSE =
      "{\n"
          + "  \"expires_in\": 3600,\n"
          + "  \"access_token\": \"test-access-token\",\n"
          + "  \"token_type\": \"Bearer\"\n"
          + "}";
  public static final String GCP_TEST_URL =
      "jdbc:databricks://4371047901336987.7.gcp.databricks.com:443/default;transportMode=http;AuthMech=11;Auth_Flow=1;httpPath=/sql/1.0/warehouses/dd5955aacf3f09e5;GoogleServiceAccount=abc-compute@developer.gserviceaccount.com;";
  public static final String VALID_URL_1 =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;LogLevel=debug;LogPath=./test1;auth_flow=2";
  public static final String VALID_URL_2 =
      "jdbc:databricks://adb-565656.azuredatabricks.net/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;LogLevel=invalid;EnableQueryResultLZ4Compression=1;UseThriftClient=0";
  public static final String VALID_URL_3 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=0;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;EnableQueryResultLZ4Compression=0;UseThriftClient=1;LogLevel=1234";
  public static final String VALID_URL_4 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;QueryResultCompressionType=1;EnableDirectResults=1;";
  public static final String VALID_URL_5 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:4473;ssl=0;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;QueryResultCompressionType=1;EnableDirectResults=0";
  public static final String VALID_URL_6 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:4473/schemaName;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;ConnCatalog=catalogName;ConnSchema=schemaName;QueryResultCompressionType=1";
  public static final String VALID_URL_7 =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/endpoints/erg6767gg;LogLevel=debug;LogPath=./test1;auth_flow=2;enablearrow=0";
  public static final String VALID_URL_8 =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;ssl=1;port=123;AuthMech=3;"
          + "httpPath=/sql/1.0/endpoints/erg6767gg;LogLevel=debug;LogPath=./test1;auth_flow=2;enablearrow=0;"
          + "OAuth2AuthorizationEndPoint=authEndpoint;OAuth2TokenEndpoint=tokenEndpoint;"
          + "OAuthDiscoveryURL=testDiscovery;discovery_mode=1;UseJWTAssertion=1;auth_scope=test_scope;"
          + "auth_kid=test_kid;Auth_JWT_Key_Passphrase=test_phrase;Auth_JWT_Key_File=test_key_file;"
          + "Auth_JWT_Alg=test_algo";
  public static final String VALID_URL_WITH_INVALID_COMPRESSION_TYPE =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;QueryResultCompressionType=234";
  public static final String INVALID_URL_1 =
      "jdbc:oracle://azuredatabricks.net/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;";
  public static final String INVALID_URL_2 =
      "http:databricks://azuredatabricks.net/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;";
  public static final String INVALID_URL_3 =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;ssl=1;port=alphabetical;AuthMech=3;httpPath=/sql/1.0/endpoints/erg6767gg;LogLevel=debug;LogPath=./test1;auth_flow=2;enablearrow=0";
  public static final String VALID_TEST_URL = "jdbc:databricks://test";
  public static final String VALID_CLUSTER_URL =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;loglevel=3";
  public static final String INVALID_CLUSTER_URL =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;httpPath=sql/protocolv1/oo/6051921418418893/1115-130834-ms4m0yv;AuthMech=3";
  public static final String VALID_BASE_URL_1 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;";
  public static final String VALID_BASE_URL_2 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default";
  public static final String VALID_BASE_URL_3 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443";
  public static final String VALID_URL_WITH_PROXY =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;UseProxy=1;ProxyHost=127.0.0.1;ProxyPort=8080;ProxyAuth=1;ProxyUID=proxyUser;ProxyPwd=proxyPassword;";
  public static final String VALID_URL_WITH_PROXY_AND_CF_PROXY =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;UseSystemProxy=1;UseProxy=1;ProxyHost=127.0.0.1;ProxyPort=8080;ProxyAuth=1;ProxyUID=proxyUser;ProxyPwd=proxyPassword;UseCFProxy=1;CFProxyHost=127.0.1.2;CFProxyPort=8081;CFProxyAuth=2;CFProxyUID=cfProxyUser;CFProxyPwd=cfProxyPassword;";
  public static final String VALID_URL_POLLING =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:4473;ssl=1;asyncexecpollinterval=500;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;QueryResultCompressionType=1";
  public static final String VALID_URL_WITH_STAGING_ALLOWED_PATH =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;LogLevel=debug;LogPath=./test1;auth_flow=2;StagingAllowedLocalPaths=/tmp";
  public static final String VALID_URL_WITH_VOLUME_ALLOWED_PATH =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;LogLevel=debug;LogPath=./test1;auth_flow=2;VolumeOperationAllowedLocalPaths=/tmp2";
  public static final List<TSparkArrowBatch> ARROW_BATCH_LIST =
      Collections.singletonList(
          new TSparkArrowBatch().setRowCount(0).setBatch(new byte[] {65, 66, 67}));
}
