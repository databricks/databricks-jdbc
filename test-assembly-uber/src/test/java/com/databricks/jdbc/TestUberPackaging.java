package com.databricks.jdbc;

import com.databricks.internal.apache.commons.codec.binary.Base64;
import com.databricks.internal.apache.commons.io.IOUtils;
import com.databricks.internal.apache.commons.lang3.StringUtils;
import com.databricks.internal.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import com.databricks.internal.apache.hc.client5.http.impl.classic.HttpClients;
import com.databricks.internal.apache.hc.core5.http.HttpStatus;
import com.databricks.internal.apache.thrift.TException;
import com.databricks.internal.bouncycastle.jce.provider.BouncyCastleProvider;
import com.databricks.internal.fasterxml.jackson.databind.ObjectMapper;
import com.databricks.internal.google.common.collect.ImmutableList;
import com.databricks.internal.google.flatbuffers.FlatBufferBuilder;
import com.databricks.internal.google.gson.Gson;
import com.databricks.internal.google.protobuf.ByteString;
import com.databricks.internal.io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import com.databricks.internal.io.grpc.Context;
import com.databricks.internal.io.netty.buffer.ByteBufAllocator;
import com.databricks.internal.io.vavr.collection.List;
import com.databricks.internal.json.JSONObject;
import com.databricks.internal.jts.geom.GeometryFactory;
import com.databricks.internal.nimbusds.jose.JWSAlgorithm;
import com.databricks.internal.sdk.core.DatabricksConfig;
import com.databricks.jdbc.api.impl.arrow.ArrowBufferAllocator;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
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
import org.junit.jupiter.api.Test;

/** Test artifacts are packaged properly. */
public class TestUberPackaging {
  /** Logger instance. */
  private static final java.util.logging.Logger logger =
      Logger.getLogger(TestUberPackaging.class.getName());

  /** Test packages are shaded as expected. */
  @Test
  public void testThinPackaging() {
    // Test that the "arrow" package is relocated.
    com.databricks.internal.apache.arrow.memory.BufferAllocator bufferAllocator =
        ArrowBufferAllocator.getBufferAllocator();
    logger.info("Shaded buffer allocator " + bufferAllocator);

    // Test that jackson packages are relocated.
    ObjectMapper jacksonMapper = new ObjectMapper();
    logger.info("Shaded Jackson ObjectMapper: " + jacksonMapper);

    // Test that guava is relocated.
    ImmutableList<String> guavaList = ImmutableList.of("test");
    logger.info("Shaded Guava ImmutableList: " + guavaList);

    // Test that protobuf is relocated.
    ByteString protoByteString = ByteString.copyFromUtf8("test");
    logger.info("Shaded Protobuf ByteString: " + protoByteString);

    // Test that commons-lang3 is relocated.
    String commonsResult = StringUtils.upperCase("test");
    logger.info("Shaded Commons-Lang3 result: " + commonsResult);

    // Test that commons-codec is relocated (org.apache.commons.codec ->
    // com.databricks.internal.apache.commons.codec).
    byte[] commonsCodec = Base64.encodeBase64("test".getBytes());
    logger.info("Shaded Commons-Codec Base64: " + new String(commonsCodec));

    // Test that commons-io is relocated.
    try {
      String commonsIo = IOUtils.toString(new ByteArrayInputStream("test".getBytes()), "UTF-8");
      logger.info("Shaded Commons-IO result: " + commonsIo);
    } catch (IOException e) {
      throw new RuntimeException("Failed to test Commons-IO shading", e);
    }

    // Test that httpclient5 is relocated (org.apache.hc.client5 ->
    // com.databricks.internal.apache.hc.client5).
    HttpClientBuilder httpClientBuilder = HttpClients.custom();
    logger.info("Shaded HttpClient5 builder: " + httpClientBuilder);

    // Test that httpcore5 is relocated (org.apache.hc.core5 ->
    // com.databricks.internal.apache.hc.core5).
    int httpStatus = HttpStatus.SC_OK;
    logger.info("Shaded HttpCore5 status: " + httpStatus);

    // Test that thrift is relocated.
    TException thriftException = new TException("test");
    logger.info("Shaded Thrift TException: " + thriftException.getMessage());

    // Test that gson is relocated.
    Gson gson = new Gson();
    logger.info("Shaded Gson: " + gson);

    // Test that flatbuffers is relocated.
    FlatBufferBuilder flatBuilder = new FlatBufferBuilder();
    logger.info("Shaded FlatBuffers: " + flatBuilder);

    // Test that netty is relocated (io.netty -> com.databricks.internal.io.netty).
    ByteBufAllocator nettyAllocator = ByteBufAllocator.DEFAULT;
    logger.info("Shaded Netty ByteBufAllocator: " + nettyAllocator);

    // Test that grpc is relocated (io.grpc -> com.databricks.internal.io.grpc).
    Context grpcContext = Context.current();
    logger.info("Shaded gRPC Context: " + grpcContext);

    // Test that bouncycastle is relocated (org.bouncycastle ->
    // com.databricks.internal.bouncycastle).
    BouncyCastleProvider bcProvider = new BouncyCastleProvider();
    logger.info("Shaded BouncyCastle Provider: " + bcProvider.getName());

    // Test that resilience4j is relocated (io.github.resilience4j ->
    // com.databricks.internal.io.github.resilience4j).
    CircuitBreakerConfig cbConfig = CircuitBreakerConfig.ofDefaults();
    logger.info("Shaded Resilience4j CircuitBreakerConfig: " + cbConfig);

    // Test that vavr is relocated (io.vavr -> com.databricks.internal.io.vavr).
    List<String> vavrList = List.of("test");
    logger.info("Shaded Vavr List: " + vavrList);

    // Test that JTS is relocated (org.locationtech.jts -> com.databricks.internal.jts).
    GeometryFactory jtsFactory = new GeometryFactory();
    logger.info("Shaded JTS GeometryFactory: " + jtsFactory);

    // Test that Databricks SDK is relocated (com.databricks.sdk -> com.databricks.internal.sdk).
    Class<?> sdkClass = DatabricksConfig.class;
    logger.info("Shaded Databricks SDK class: " + sdkClass.getName());

    // Test that JSON is relocated (org.json -> com.databricks.internal.json).
    JSONObject jsonObject = new JSONObject().put("key", "value");
    logger.info("Shaded JSON object: " + jsonObject);

    // Test that Nimbus JOSE JWT is relocated (com.nimbusds -> com.databricks.internal.nimbusds).
    JWSAlgorithm jwsAlgorithm = JWSAlgorithm.HS256;
    logger.info("Shaded Nimbus JWSAlgorithm: " + jwsAlgorithm);
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
