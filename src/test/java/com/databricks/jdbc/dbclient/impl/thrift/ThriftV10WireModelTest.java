package com.databricks.jdbc.dbclient.impl.thrift;

import static com.databricks.jdbc.common.EnvironmentVariables.JDBC_THRIFT_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.model.client.thrift.generated.TExecuteStatementReq;
import com.databricks.jdbc.model.client.thrift.generated.THandleIdentifier;
import com.databricks.jdbc.model.client.thrift.generated.TProtocolVersion;
import com.databricks.jdbc.model.client.thrift.generated.TSessionHandle;
import com.databricks.jdbc.model.client.thrift.generated.TSparkParameter;
import com.databricks.jdbc.model.client.thrift.generated.TSparkParameterValue;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Test;

class ThriftV10WireModelTest {

  @Test
  void testSparkProtocolV10Value() {
    assertEquals(0xA50A, TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V10.getValue());
    assertEquals(
        TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V10, TProtocolVersion.findByValue(0xA50A));
    assertEquals(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V10, JDBC_THRIFT_VERSION);
  }

  @Test
  void testBatchParametersSerializationRoundTrip() throws Exception {
    TSessionHandle sessionHandle =
        new TSessionHandle(
            new THandleIdentifier(
                ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2})));
    List<List<TSparkParameter>> batchParameters =
        List.of(
            List.of(
                new TSparkParameter()
                    .setOrdinal(0)
                    .setType("STRING")
                    .setValue(TSparkParameterValue.stringValue("first"))),
            List.of(
                new TSparkParameter()
                    .setOrdinal(0)
                    .setType("STRING")
                    .setValue(TSparkParameterValue.stringValue("second"))));
    TExecuteStatementReq request =
        new TExecuteStatementReq(sessionHandle, "INSERT INTO t VALUES (?)")
            .setBatchParameters(batchParameters);

    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    TExecuteStatementReq deserialized = new TExecuteStatementReq();
    new TDeserializer(new TBinaryProtocol.Factory())
        .deserialize(deserialized, serializer.serialize(request));

    assertEquals(batchParameters, deserialized.getBatchParameters());
  }
}
