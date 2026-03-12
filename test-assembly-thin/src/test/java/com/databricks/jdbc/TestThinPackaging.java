package com.databricks.jdbc;

import com.databricks.jdbc.api.impl.arrow.ArrowBufferAllocator;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.sdk.core.DatabricksConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.nimbusds.jose.JWSAlgorithm;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.grpc.Context;
import io.netty.buffer.ByteBufAllocator;
import io.vavr.collection.List;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.thrift.TException;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.GeometryFactory;

/** Test artifacts are packaged properly. */
public class TestThinPackaging {
  /** Logger instance. */
  private static final Logger logger = Logger.getLogger(TestThinPackaging.class.getName());

  /** Test packages are shaded as expected. */
  @Test
  public void testThinPackaging() {
    // Test that Arrow packages is relocated.
    com.databricks.internal.apache.arrow.memory.BufferAllocator bufferAllocator =
        ArrowBufferAllocator.getBufferAllocator();
    logger.info("Shaded buffer allocator " + bufferAllocator);

    // Test that jackson packages are not relocated.
    ObjectMapper jacksonMapper = new ObjectMapper();
    logger.info("Jackson ObjectMapper: " + jacksonMapper);

    // Test that guava is not relocated.
    ImmutableList<String> guavaList = ImmutableList.of("test");
    logger.info("Guava ImmutableList: " + guavaList);

    // Test that protobuf is not relocated.
    ByteString protoByteString = ByteString.copyFromUtf8("test");
    logger.info("Protobuf ByteString: " + protoByteString);

    // Test that commons-lang3 is not relocated.
    String commonsResult = StringUtils.upperCase("test");
    logger.info("Commons-Lang3 result: " + commonsResult);

    // Test that commons-codec is not relocated.
    byte[] commonsCodec = Base64.encodeBase64("test".getBytes());
    logger.info("Commons-Codec Base64: " + new String(commonsCodec));

    // Test that commons-io is not relocated.
    try {
      String commonsIo = IOUtils.toString(new ByteArrayInputStream("test".getBytes()), "UTF-8");
      logger.info("Commons-IO result: " + commonsIo);
    } catch (IOException e) {
      throw new RuntimeException("Failed to test Commons-IO", e);
    }

    // Test that httpclient5 is not relocated.
    HttpClientBuilder httpClientBuilder = HttpClients.custom();
    logger.info("HttpClient5 builder: " + httpClientBuilder);

    // Test that httpcore5 is not relocated.
    int httpStatus = HttpStatus.SC_OK;
    logger.info("HttpCore5 status: " + httpStatus);

    // Test that thrift is not relocated.
    TException thriftException = new TException("test");
    logger.info("Thrift TException: " + thriftException.getMessage());

    // Test that gson is not relocated.
    Gson gson = new Gson();
    logger.info("Gson: " + gson);

    // Test that flatbuffers is not relocated.
    FlatBufferBuilder flatBuilder = new FlatBufferBuilder();
    logger.info("FlatBuffers: " + flatBuilder);

    // Test that netty is not relocated.
    ByteBufAllocator nettyAllocator = ByteBufAllocator.DEFAULT;
    logger.info("Netty ByteBufAllocator: " + nettyAllocator);

    // Test that grpc is not relocated.
    Context grpcContext = Context.current();
    logger.info("gRPC Context: " + grpcContext);

    // Test that bouncycastle is not relocated.
    BouncyCastleProvider bcProvider = new BouncyCastleProvider();
    logger.info("BouncyCastle Provider: " + bcProvider.getName());

    // Test that resilience4j is not relocated.
    CircuitBreakerConfig cbConfig = CircuitBreakerConfig.ofDefaults();
    logger.info("Resilience4j CircuitBreakerConfig: " + cbConfig);

    // Test that vavr is not relocated.
    List<String> vavrList = List.of("test");
    logger.info("Vavr List: " + vavrList);

    // Test that JTS is not relocated.
    GeometryFactory jtsFactory = new GeometryFactory();
    logger.info("JTS GeometryFactory: " + jtsFactory);

    // Test that Databricks SDK is not relocated.
    Class<?> sdkClass = DatabricksConfig.class;
    logger.info("Databricks SDK class: " + sdkClass.getName());

    // Test that JSON is not relocated.
    JSONObject jsonObject = new JSONObject().put("key", "value");
    logger.info("JSON object: " + jsonObject);

    // Test that Nimbus JOSE JWT is not relocated.
    JWSAlgorithm jwsAlgorithm = JWSAlgorithm.HS256;
    logger.info("Nimbus JWSAlgorithm: " + jwsAlgorithm);
  }

  /** Test large query execution with Arrow result format works. */
  @Test
  public void executeLargeQuery() throws SQLException {
    Map<String, Object> params = new HashMap<>();
    params.put(DatabricksJdbcUrlParams.ENABLE_ARROW.getParamName(), "1");
    params.put(DatabricksJdbcUrlParams.USE_THRIFT_CLIENT.getParamName(), "0");

    try (Connection connection = connect(params)) {
      try (Statement statement = connection.createStatement()) {
        final String sql = "SELECT * FROM samples.tpch.lineitem where 1 = 0";
        ResultSet result = statement.executeQuery(sql);
        int totalRows = 0;
        while (result.next()) {
          if (totalRows % 100_000 == 0) {
            logger.info("Processed " + totalRows + " rows");
          }
          totalRows++;
        }

        logger.info("Total " + totalRows + " rows processed");
      }
    }
  }

  private Connection connect(Map<String, Object> urlParams) throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", getDatabricksUser());
    props.setProperty("password", getDatabricksToken());
    for (Map.Entry<String, Object> entry : urlParams.entrySet()) {
      props.setProperty(entry.getKey(), entry.getValue().toString());
    }

    String url = getDogfoodJDBCUrl();

    return new com.databricks.client.jdbc.Driver().connect(url, props);
  }

  private String getDogfoodJDBCUrl() {
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=1;AuthMech=3;httpPath=%s";
    String host = getDatabricksHost();
    String httpPath = getDatabricksHttpPath();

    return String.format(template, host, httpPath);
  }

  private String getDatabricksHttpPath() {
    return System.getenv("DATABRICKS_HTTP_PATH");
  }

  private String getDatabricksHost() {
    return System.getenv("DATABRICKS_HOST");
  }

  private String getDatabricksUser() {
    return Optional.ofNullable(System.getenv("DATABRICKS_USER")).orElse("token");
  }

  private String getDatabricksToken() {
    return System.getenv("DATABRICKS_TOKEN");
  }
}
