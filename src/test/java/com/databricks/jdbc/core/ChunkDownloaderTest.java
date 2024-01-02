package com.databricks.jdbc.core;

import static java.lang.Math.min;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.core.ArrowResultChunk.DownloadStatus;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.service.sql.*;
import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ChunkDownloaderTest {

  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final String SESSION_ID = "session_id";
  private static final String STATEMENT_ID = "statement_id";
  private static final String STATEMENT = "select 1";
  private static final String JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";
  private static final String CHUNK_URL_PREFIX = "chunk.databricks.com/";
  private static final long TOTAL_CHUNKS = 5;
  private static final long TOTAL_ROWS = 90;
  private static final long TOTAL_BYTES = 1000;

  private int rowsInRecordBatch = 20;
  private Random random = new Random();

  @Mock StatementExecutionService statementExecutionService;
  @Mock DatabricksSdkClient mockedSdkClient;
  @Mock IDatabricksHttpClient mockHttpClient;
  @Mock CloseableHttpResponse httpResponse;
  @Mock HttpEntity httpEntity;
  @Mock ApiClient apiClient;

  @Test
  public void testInitEmptyChunkDownloader() throws Exception {
    ResultManifest resultManifest =
        new ResultManifest()
            .setTotalChunkCount(0L)
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()));
    ResultData resultData = new ResultData().setExternalLinks(new ArrayList<>());
    assertDoesNotThrow(
        () -> new ChunkDownloader(STATEMENT_ID, resultManifest, resultData, null, null));
  }

  // @Test
  public void testInitChunkDownloader() throws Exception {
    ResultManifest resultManifest = getResultManifest();
    ResultData resultData = getResultData();

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksSession session =
        new DatabricksSession(
            connectionContext,
            new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient));
    setupMockResponse();
    when(mockHttpClient.execute(isA(HttpUriRequest.class))).thenReturn(httpResponse);

    ChunkDownloader chunkDownloader =
        new ChunkDownloader(STATEMENT_ID, resultManifest, resultData, session, mockHttpClient);
    verify(statementExecutionService, times(3))
        .getStatementResultChunkN(isA(GetStatementResultChunkNRequest.class));

    assertEquals(4, chunkDownloader.getTotalChunksInMemory());
    assertTrue(chunkDownloader.hasNextChunk());

    for (long chunkResultIndex = 0L; chunkResultIndex < TOTAL_CHUNKS; chunkResultIndex++) {
      assertTrue(chunkDownloader.next());
      assertChunkResult(chunkDownloader.getChunk(), chunkResultIndex);
    }
    // TDOD: assert urls as well
    verify(mockHttpClient, times(5)).execute(isA(HttpUriRequest.class));
  }

  private ResultData getResultData() {
    return new ResultData().setExternalLinks(getChunkLinks(0L, false));
  }

  private ResultManifest getResultManifest() {
    List<BaseChunkInfo> chunks = new ArrayList<>();
    for (long chunkIndex = 0; chunkIndex < TOTAL_CHUNKS; chunkIndex++) {
      BaseChunkInfo chunkInfo =
          new BaseChunkInfo()
              .setChunkIndex(chunkIndex)
              .setByteCount(200L)
              .setRowOffset(chunkIndex * 20);
      if (chunkIndex < TOTAL_CHUNKS - 1) {
        chunkInfo.setRowCount(20L);
      } else {
        chunkInfo.setRowCount(10L);
      }
      chunks.add(chunkInfo);
    }
    return new ResultManifest()
        .setTotalChunkCount(TOTAL_CHUNKS)
        .setTotalRowCount(TOTAL_ROWS)
        .setTotalByteCount(TOTAL_BYTES)
        .setChunks(chunks)
        .setSchema(new ResultSchema().setColumns(new ArrayList<>()));
  }

  private void setupMockResponse() throws Exception {
    Schema schema = createTestSchema();
    Object[][] testData = createTestData(schema, 20);
    File arrowFile =
        createTestArrowFile("TestFile", schema, testData, new RootAllocator(Integer.MAX_VALUE));

    when(httpResponse.getEntity()).thenReturn(httpEntity);
    when(httpEntity.getContent()).thenAnswer(invocation -> new FileInputStream(arrowFile));
  }

  private List<ExternalLink> getChunkLinks(long chunkIndex, boolean isLast) {
    List<ExternalLink> chunkLinks = new ArrayList<>();
    ExternalLink chunkLink =
        new ExternalLink()
            .setChunkIndex(chunkIndex)
            .setExternalLink(CHUNK_URL_PREFIX + chunkIndex)
            .setExpiration(Instant.now().plusSeconds(3600L).toString());
    if (!isLast) {
      chunkLink.setNextChunkIndex(chunkIndex + 1);
    }
    chunkLinks.add(chunkLink);
    return chunkLinks;
  }

  private void assertChunkResult(ArrowResultChunk chunk, long chunkIndex) {
    long expectedRows = chunkIndex < 4 ? 20L : 10L;
    long expectedRowsOffSet = chunkIndex * 20L;
    assertEquals(chunkIndex, chunk.getChunkIndex());
    assertEquals(expectedRows, chunk.numRows);
    assertEquals(expectedRowsOffSet, chunk.rowOffset);
    assertEquals(CHUNK_URL_PREFIX + chunkIndex, chunk.getChunkUrl());

    assertNotNull(chunk.getDownloadFinishTime());
    assertEquals(DownloadStatus.DOWNLOAD_SUCCEEDED, chunk.getStatus());
  }

  private File createTestArrowFile(
      String fileName, Schema schema, Object[][] testData, RootAllocator allocator)
      throws IOException {
    File file = new File(fileName);
    int cols = testData.length;
    int rows = testData[0].length;
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
    ArrowWriter writer =
        new ArrowStreamWriter(
            vectorSchemaRoot,
            new DictionaryProvider.MapDictionaryProvider(),
            new FileOutputStream(file));
    writer.start();
    for (int j = 0; j < rows; j += rowsInRecordBatch) {
      int rowsToAddToRecordBatch = min(rowsInRecordBatch, rows - j);
      vectorSchemaRoot.setRowCount(rowsToAddToRecordBatch);
      for (int i = 0; i < cols; i++) {
        Types.MinorType type = Types.getMinorTypeForArrowType(schema.getFields().get(i).getType());
        FieldVector fieldVector = vectorSchemaRoot.getFieldVectors().get(i);
        if (type.equals(Types.MinorType.INT)) {
          IntVector intVector = (IntVector) fieldVector;
          intVector.setInitialCapacity(rowsToAddToRecordBatch);
          for (int k = 0; k < rowsToAddToRecordBatch; k++) {
            intVector.set(k, 1, (int) testData[i][j + k]);
          }
        } else if (type.equals(Types.MinorType.FLOAT8)) {
          Float8Vector float8Vector = (Float8Vector) fieldVector;
          float8Vector.setInitialCapacity(rowsToAddToRecordBatch);
          for (int k = 0; k < rowsToAddToRecordBatch; k++) {
            float8Vector.set(k, 1, (double) testData[i][j + k]);
          }
        }
        fieldVector.setValueCount(rowsToAddToRecordBatch);
      }
      writer.writeBatch();
    }
    return file;
  }

  private Schema createTestSchema() {
    List<Field> fieldList = new ArrayList<>();
    FieldType fieldType1 = new FieldType(false, Types.MinorType.INT.getType(), null);
    FieldType fieldType2 = new FieldType(false, Types.MinorType.FLOAT8.getType(), null);
    fieldList.add(new Field("Field1", fieldType1, null));
    fieldList.add(new Field("Field2", fieldType2, null));
    return new Schema(fieldList);
  }

  private Object[][] createTestData(Schema schema, int rows) {
    int cols = schema.getFields().size();
    Object[][] data = new Object[cols][rows];
    for (int i = 0; i < cols; i++) {
      Types.MinorType type = Types.getMinorTypeForArrowType(schema.getFields().get(i).getType());
      if (type.equals(Types.MinorType.INT)) {
        for (int j = 0; j < rows; j++) {
          data[i][j] = random.nextInt();
        }
      } else if (type.equals(Types.MinorType.FLOAT8)) {
        for (int j = 0; j < rows; j++) {
          data[i][j] = random.nextDouble();
        }
      }
    }
    return data;
  }
}
