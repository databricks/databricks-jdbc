package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.TestConstants.*;
import static java.lang.Math.min;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.impl.DatabricksSession;
import com.databricks.jdbc.common.CompressionType;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.model.client.thrift.generated.TRowSet;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.ResultSchema;
import com.google.common.collect.ImmutableList;
import java.io.*;
import java.time.Instant;
import java.util.*;
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
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ArrowStreamResultTest {
  private final List<BaseChunkInfo> chunkInfos = new ArrayList<>();
  @Mock TGetResultSetMetadataResp metadataResp;
  @Mock TRowSet resultData;
  @Mock IDatabricksSession session;
  private final int numberOfChunks = 10;
  private final Random random = new Random();
  private final long rowsInChunk = 110L;
  private static final String JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";
  private static final String CHUNK_URL_PREFIX = "chunk.databricks.com/";
  private static final String STATEMENT_ID = "statement_id";
  @Mock DatabricksSdkClient mockedSdkClient;
  @Mock IDatabricksHttpClient mockHttpClient;
  @Mock CloseableHttpResponse httpResponse;
  @Mock HttpEntity httpEntity;
  @Mock StatusLine mockedStatusLine;

  @BeforeEach
  public void setup() throws Exception {
    setupChunks();
  }

  @Test
  public void testInitEmptyArrowStreamResult() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, new Properties());
    when(session.getConnectionContext()).thenReturn(connectionContext);
    ResultManifest resultManifest =
        new ResultManifest()
            .setTotalChunkCount(0L)
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()).setColumnCount(0L));
    ResultData resultData = new ResultData().setExternalLinks(new ArrayList<>());
    ArrowStreamResult result =
        new ArrowStreamResult(resultManifest, resultData, STATEMENT_ID, session);
    assertDoesNotThrow(result::close);
    assertFalse(result.hasNext());
  }

  @Test
  public void testIteration() throws Exception {
    // Arrange
    ResultManifest resultManifest =
        new ResultManifest()
            .setTotalChunkCount((long) this.numberOfChunks)
            .setTotalRowCount(this.numberOfChunks * 110L)
            .setTotalByteCount(1000L)
            .setCompressionType(CompressionType.NONE)
            .setChunks(this.chunkInfos)
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()).setColumnCount(0L));

    ResultData resultData = new ResultData().setExternalLinks(getChunkLinks(0L, false));

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, new Properties());
    DatabricksSession session = new DatabricksSession(connectionContext, mockedSdkClient);
    setupMockResponse();
    setupResultChunkMocks();
    when(mockHttpClient.execute(isA(HttpUriRequest.class))).thenReturn(httpResponse);

    ArrowStreamResult result =
        new ArrowStreamResult(resultManifest, resultData, STATEMENT_ID, session, mockHttpClient);

    // Act & Assert
    for (int i = 0; i < this.numberOfChunks; ++i) {
      // Since the first row of the chunk is null
      for (int j = 0; j < (this.rowsInChunk); ++j) {
        assertTrue(result.hasNext());
        assertTrue(result.next());
      }
    }
    assertFalse(result.hasNext());
    assertFalse(result.next());
  }

  @Test
  public void testInlineArrow() throws DatabricksSQLException {
    when(metadataResp.getSchema()).thenReturn(TEST_TABLE_SCHEMA);
    ArrowStreamResult result =
        new ArrowStreamResult(metadataResp, resultData, true, TEST_STATEMENT_ID, session);
    assertEquals(-1, result.getCurrentRow());
    assertTrue(result.hasNext());
    assertFalse(result.next());
    assertEquals(0, result.getCurrentRow());
    assertFalse(result.hasNext());
    assertDoesNotThrow(result::close);
    assertFalse(result.hasNext());
  }

  @Test
  public void testCloudFetchArrow() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, new Properties());
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(metadataResp.getSchema()).thenReturn(TEST_TABLE_SCHEMA);
    TSparkArrowResultLink resultLink = new TSparkArrowResultLink().setFileLink(TEST_STRING);
    when(resultData.getResultLinks()).thenReturn(Collections.singletonList(resultLink));
    when(resultData.getResultLinksSize()).thenReturn(1);
    ArrowStreamResult result =
        new ArrowStreamResult(
            metadataResp, resultData, false, TEST_STATEMENT_ID, session, mockHttpClient);
    assertEquals(-1, result.getCurrentRow());
    assertTrue(result.hasNext());
    assertDoesNotThrow(result::close);
    assertFalse(result.hasNext());
  }

  @Test
  public void testGetObject() throws Exception {
    // Arrange
    ResultManifest resultManifest =
        new ResultManifest()
            .setTotalChunkCount((long) this.numberOfChunks)
            .setTotalRowCount(this.numberOfChunks * 110L)
            .setTotalByteCount(1000L)
            .setCompressionType(CompressionType.NONE)
            .setChunks(this.chunkInfos)
            .setSchema(
                new ResultSchema()
                    .setColumns(
                        ImmutableList.of(
                            new ColumnInfo().setTypeName(ColumnInfoTypeName.INT),
                            new ColumnInfo().setTypeName(ColumnInfoTypeName.DOUBLE)))
                    .setColumnCount(2L));

    ResultData resultData = new ResultData().setExternalLinks(getChunkLinks(0L, false));

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, new Properties());
    DatabricksSession session = new DatabricksSession(connectionContext, mockedSdkClient);

    setupMockResponse();
    when(mockHttpClient.execute(isA(HttpUriRequest.class))).thenReturn(httpResponse);

    ArrowStreamResult result =
        new ArrowStreamResult(resultManifest, resultData, STATEMENT_ID, session, mockHttpClient);

    result.next();
    Object objectInFirstColumn = result.getObject(0);
    Object objectInSecondColumn = result.getObject(1);

    assertInstanceOf(Integer.class, objectInFirstColumn);
    assertInstanceOf(Double.class, objectInSecondColumn);
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

  private void setupChunks() {
    for (int i = 0; i < this.numberOfChunks; ++i) {
      BaseChunkInfo chunkInfo =
          new BaseChunkInfo()
              .setChunkIndex((long) i)
              .setByteCount(1000L)
              .setRowOffset(i * 110L)
              .setRowCount(this.rowsInChunk);
      this.chunkInfos.add(chunkInfo);
    }
  }

  private void setupMockResponse() throws Exception {
    Schema schema = createTestSchema();
    Object[][] testData = createTestData(schema, (int) this.rowsInChunk);
    File arrowFile =
        createTestArrowFile("TestFile", schema, testData, new RootAllocator(Integer.MAX_VALUE));

    when(httpResponse.getEntity()).thenReturn(httpEntity);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(200);
    when(httpEntity.getContent()).thenAnswer(invocation -> new FileInputStream(arrowFile));
  }

  private void setupResultChunkMocks() {
    for (int chunkIndex = 1; chunkIndex < numberOfChunks; chunkIndex++) {
      boolean isLastChunk = (chunkIndex == (numberOfChunks - 1));
      when(mockedSdkClient.getResultChunks(STATEMENT_ID, chunkIndex))
          .thenReturn(getChunkLinks(chunkIndex, isLastChunk));
    }
  }

  private File createTestArrowFile(
      String fileName, Schema schema, Object[][] testData, RootAllocator allocator)
      throws IOException {
    int rowsInRecordBatch = 20;
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
          for (int currentRow = 0; currentRow < rowsToAddToRecordBatch; currentRow++) {
            float8Vector.set(currentRow, 1, (double) testData[i][j + currentRow]);
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
