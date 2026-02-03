package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.TestConstants.*;
import static java.lang.Math.min;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.impl.DatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.model.client.thrift.generated.TRowSet;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.sdk.service.sql.BaseChunkInfo;
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
  @Mock TFetchResultsResp fetchResultsResp;
  @Mock IDatabricksSession session;
  @Mock IDatabricksStatementInternal parentStatement;
  private final int numberOfChunks = 10;
  private final Random random = new Random();
  private final long rowsInChunk = 110L;
  private static final String JDBC_URL =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;";
  private static final String CHUNK_URL_PREFIX = "chunk.databricks.com/";
  private static final StatementId STATEMENT_ID = new StatementId("statement_id");
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
            .setTotalRowCount(0L)
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
            .setResultCompression(CompressionCodec.NONE)
            .setChunks(this.chunkInfos)
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()).setColumnCount(0L));

    ResultData resultData = new ResultData().setExternalLinks(getChunkLinks(0L, 0L, false));

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, new Properties());
    DatabricksSession session = new DatabricksSession(connectionContext, mockedSdkClient);
    setupMockResponse();
    setupResultChunkMocks();
    when(mockHttpClient.execute(isA(HttpUriRequest.class), eq(true))).thenReturn(httpResponse);

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
  public void testCloudFetchArrow() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, new Properties());
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(metadataResp.getSchema()).thenReturn(TEST_TABLE_SCHEMA);
    TSparkArrowResultLink resultLink = new TSparkArrowResultLink().setFileLink(TEST_STRING);
    when(resultData.getResultLinks()).thenReturn(Collections.singletonList(resultLink));
    when(fetchResultsResp.getResults()).thenReturn(resultData);
    when(fetchResultsResp.getResultSetMetadata()).thenReturn(metadataResp);
    when(parentStatement.getStatementId()).thenReturn(STATEMENT_ID);
    ArrowStreamResult result =
        new ArrowStreamResult(fetchResultsResp, parentStatement, session, mockHttpClient);
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
            .setResultCompression(CompressionCodec.NONE)
            .setChunks(this.chunkInfos)
            .setSchema(
                new ResultSchema()
                    .setColumns(
                        ImmutableList.of(
                            new ColumnInfo().setTypeName(ColumnInfoTypeName.INT),
                            new ColumnInfo().setTypeName(ColumnInfoTypeName.DOUBLE)))
                    .setColumnCount(2L));

    ResultData resultData = new ResultData().setExternalLinks(getChunkLinks(0L, 0L, false));

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, new Properties());
    DatabricksSession session = new DatabricksSession(connectionContext, mockedSdkClient);

    setupMockResponse();
    when(mockHttpClient.execute(isA(HttpUriRequest.class), eq(true))).thenReturn(httpResponse);

    ArrowStreamResult result =
        new ArrowStreamResult(resultManifest, resultData, STATEMENT_ID, session, mockHttpClient);

    result.next();
    Object objectInFirstColumn = result.getObject(0);
    Object objectInSecondColumn = result.getObject(1);

    assertInstanceOf(Integer.class, objectInFirstColumn);
    assertInstanceOf(Double.class, objectInSecondColumn);
  }

  @Test
  public void testComplexTypeHandling() {
    assertTrue(ArrowStreamResult.isComplexType(ColumnInfoTypeName.ARRAY));
    assertTrue(ArrowStreamResult.isComplexType(ColumnInfoTypeName.MAP));
    assertTrue(ArrowStreamResult.isComplexType(ColumnInfoTypeName.STRUCT));
    assertTrue(ArrowStreamResult.isComplexType(ColumnInfoTypeName.GEOMETRY));
    assertTrue(ArrowStreamResult.isComplexType(ColumnInfoTypeName.GEOGRAPHY));

    // Non-complex types should return false
    assertFalse(ArrowStreamResult.isComplexType(ColumnInfoTypeName.INT));
    assertFalse(ArrowStreamResult.isComplexType(ColumnInfoTypeName.STRING));
    assertFalse(ArrowStreamResult.isComplexType(ColumnInfoTypeName.DOUBLE));
    assertFalse(ArrowStreamResult.isComplexType(ColumnInfoTypeName.BOOLEAN));
    assertFalse(ArrowStreamResult.isComplexType(ColumnInfoTypeName.TIMESTAMP));
  }

  @Test
  public void testGeospatialTypeHandling() {
    // Geospatial types should return true
    assertTrue(ArrowStreamResult.isGeospatialType(ColumnInfoTypeName.GEOMETRY));
    assertTrue(ArrowStreamResult.isGeospatialType(ColumnInfoTypeName.GEOGRAPHY));

    // Non-geospatial types should return false
    assertFalse(ArrowStreamResult.isGeospatialType(ColumnInfoTypeName.ARRAY));
    assertFalse(ArrowStreamResult.isGeospatialType(ColumnInfoTypeName.MAP));
    assertFalse(ArrowStreamResult.isGeospatialType(ColumnInfoTypeName.STRUCT));
    assertFalse(ArrowStreamResult.isGeospatialType(ColumnInfoTypeName.INT));
    assertFalse(ArrowStreamResult.isGeospatialType(ColumnInfoTypeName.STRING));
    assertFalse(ArrowStreamResult.isGeospatialType(ColumnInfoTypeName.DOUBLE));
    assertFalse(ArrowStreamResult.isGeospatialType(ColumnInfoTypeName.BOOLEAN));
    assertFalse(ArrowStreamResult.isGeospatialType(ColumnInfoTypeName.TIMESTAMP));
  }

  private List<ExternalLink> getChunkLinks(long chunkIndex, long chunkRowOffset, boolean isLast) {
    List<ExternalLink> chunkLinks = new ArrayList<>();
    ExternalLink chunkLink =
        new ExternalLink()
            .setChunkIndex(chunkIndex)
            .setRowOffset(chunkRowOffset)
            .setExternalLink(CHUNK_URL_PREFIX + chunkIndex)
            .setExpiration(Instant.now().plusSeconds(3600L).toString())
            .setRowOffset(chunkIndex * this.rowsInChunk)
            .setRowCount(this.rowsInChunk);
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

  private void setupResultChunkMocks() throws DatabricksSQLException {
    for (int chunkIndex = 1; chunkIndex < numberOfChunks; chunkIndex++) {
      boolean isLastChunk = (chunkIndex == (numberOfChunks - 1));
      long chunkRowOffset = chunkInfos.get(chunkIndex).getRowOffset();
      when(mockedSdkClient.getResultChunks(STATEMENT_ID, chunkIndex, chunkRowOffset))
          .thenReturn(
              buildChunkLinkFetchResult(getChunkLinks(chunkIndex, chunkRowOffset, isLastChunk)));
    }
  }

  private ChunkLinkFetchResult buildChunkLinkFetchResult(List<ExternalLink> links) {
    if (links == null || links.isEmpty()) {
      return ChunkLinkFetchResult.endOfStream();
    }

    ExternalLink lastLink = links.get(links.size() - 1);
    boolean hasMore = lastLink.getNextChunkIndex() != null;
    long nextFetchIndex = hasMore ? lastLink.getNextChunkIndex() : -1;
    long nextRowOffset = lastLink.getRowOffset() + lastLink.getRowCount();

    return ChunkLinkFetchResult.of(links, hasMore, nextFetchIndex, nextRowOffset);
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

  @Test
  public void testGeospatialTypeWithGeoSpatialSupportDisabled() throws Exception {
    // Setup connection context with geospatial support disabled
    // (EnableComplexDatatypeSupport=1, but EnableGeoSpatialSupport=0)
    Properties props = new Properties();
    props.setProperty("EnableComplexDatatypeSupport", "1");
    props.setProperty("EnableGeoSpatialSupport", "0");
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, props);
    when(session.getConnectionContext()).thenReturn(connectionContext);

    // Verify the flags are set correctly
    assertTrue(connectionContext.isComplexDatatypeSupportEnabled());
    assertFalse(connectionContext.isGeoSpatialSupportEnabled());

    // Test with GEOMETRY column
    List<ColumnInfo> geometryColumnInfos = new ArrayList<>();
    geometryColumnInfos.add(
        new ColumnInfo()
            .setName("geometry_col")
            .setTypeText("GEOMETRY")
            .setTypeName(ColumnInfoTypeName.GEOMETRY));

    ResultManifest geometryManifest =
        new ResultManifest()
            .setTotalChunkCount(0L)
            .setTotalRowCount(1L)
            .setSchema(new ResultSchema().setColumns(geometryColumnInfos).setColumnCount(1L));
    ResultData geometryData = new ResultData().setExternalLinks(new ArrayList<>());

    ArrowStreamResult geometryResult =
        new ArrowStreamResult(geometryManifest, geometryData, STATEMENT_ID, session);

    // Verify that GEOMETRY type is converted to STRING when geospatial support is disabled
    // The actual conversion happens in getObject(), but we're testing the flag behavior here
    assertFalse(geometryResult.hasNext());

    // Test with GEOGRAPHY column
    List<ColumnInfo> geographyColumnInfos = new ArrayList<>();
    geographyColumnInfos.add(
        new ColumnInfo()
            .setName("geography_col")
            .setTypeText("GEOGRAPHY")
            .setTypeName(ColumnInfoTypeName.GEOGRAPHY));

    ResultManifest geographyManifest =
        new ResultManifest()
            .setTotalChunkCount(0L)
            .setTotalRowCount(1L)
            .setSchema(new ResultSchema().setColumns(geographyColumnInfos).setColumnCount(1L));
    ResultData geographyData = new ResultData().setExternalLinks(new ArrayList<>());

    ArrowStreamResult geographyResult =
        new ArrowStreamResult(geographyManifest, geographyData, STATEMENT_ID, session);

    // Verify that GEOGRAPHY type is handled when geospatial support is disabled
    assertFalse(geographyResult.hasNext());
  }

  @Test
  public void testGeospatialTypeWithBothFlagsEnabled() throws Exception {
    // Setup connection context with both complex datatype and geospatial support enabled
    Properties props = new Properties();
    props.setProperty("EnableComplexDatatypeSupport", "1");
    props.setProperty("EnableGeoSpatialSupport", "1");
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, props);

    // Verify both flags are enabled
    assertTrue(connectionContext.isComplexDatatypeSupportEnabled());
    assertTrue(connectionContext.isGeoSpatialSupportEnabled());
  }

  @Test
  public void testGeospatialSupportRequiresComplexDatatypeSupport() throws Exception {
    // Test that EnableGeoSpatialSupport=1 alone (without EnableComplexDatatypeSupport) doesn't
    // enable geospatial
    Properties props = new Properties();
    props.setProperty("EnableComplexDatatypeSupport", "0");
    props.setProperty("EnableGeoSpatialSupport", "1");
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, props);

    // Verify that geospatial support is disabled because complex datatype support is disabled
    assertFalse(connectionContext.isComplexDatatypeSupportEnabled());
    assertFalse(connectionContext.isGeoSpatialSupportEnabled());
  }

  @Test
  public void testNullComplexTypeWithComplexDatatypeSupportDisabled() throws Exception {
    // Setup connection context with complex datatype support disabled
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, new Properties());
    when(session.getConnectionContext()).thenReturn(connectionContext);
    // Complex datatype support is disabled by default

    // Create result manifest with a struct column
    List<ColumnInfo> columnInfos = new ArrayList<>();
    columnInfos.add(
        new ColumnInfo()
            .setPosition(0L)
            .setName("struct_col")
            .setTypeName(ColumnInfoTypeName.STRUCT));

    ResultSchema schema = new ResultSchema().setColumns(columnInfos).setColumnCount(1L);

    ResultManifest resultManifest =
        new ResultManifest().setTotalChunkCount(0L).setTotalRowCount(1L).setSchema(schema);

    ResultData resultData = new ResultData().setExternalLinks(new ArrayList<>());

    // Create the ArrowStreamResult
    ArrowStreamResult result =
        new ArrowStreamResult(resultManifest, resultData, STATEMENT_ID, session);

    // Verify no exception is thrown when closing
    assertDoesNotThrow(result::close);
  }

  // ==================== StreamingChunkProvider Instantiation Tests ====================

  @Test
  public void testStreamingChunkProviderEnabledForSeaResult() throws Exception {
    // Enable StreamingChunkProvider via connection property
    Properties props = new Properties();
    props.setProperty("EnableStreamingChunkProvider", "1");
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, props);

    assertTrue(
        connectionContext.isStreamingChunkProviderEnabled(),
        "StreamingChunkProvider should be enabled via property");

    DatabricksSession localSession = new DatabricksSession(connectionContext, mockedSdkClient);

    // Setup result manifest with external links (triggers remote chunk provider path)
    ResultManifest resultManifest =
        new ResultManifest()
            .setTotalChunkCount(1L)
            .setTotalRowCount(110L)
            .setTotalByteCount(1000L)
            .setResultCompression(CompressionCodec.NONE)
            .setChunks(this.chunkInfos.subList(0, 1))
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()).setColumnCount(0L));

    ResultData localResultData = new ResultData().setExternalLinks(getChunkLinks(0L, 0L, true));

    setupMockResponse();
    when(mockHttpClient.execute(isA(HttpUriRequest.class), eq(true))).thenReturn(httpResponse);

    ArrowStreamResult result =
        new ArrowStreamResult(
            resultManifest, localResultData, STATEMENT_ID, localSession, mockHttpClient);

    // Verify result was created successfully with StreamingChunkProvider
    assertNotNull(result);
    assertTrue(result.hasNext(), "Result should have data");
    assertTrue(result.next());
    assertDoesNotThrow(result::close);
  }

  @Test
  public void testStreamingChunkProviderDisabledUsesRemoteChunkProvider() throws Exception {
    // Default properties - StreamingChunkProvider disabled
    Properties props = new Properties();
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, props);

    assertFalse(
        connectionContext.isStreamingChunkProviderEnabled(),
        "StreamingChunkProvider should be disabled by default");

    DatabricksSession localSession = new DatabricksSession(connectionContext, mockedSdkClient);

    ResultManifest resultManifest =
        new ResultManifest()
            .setTotalChunkCount(1L)
            .setTotalRowCount(110L)
            .setTotalByteCount(1000L)
            .setResultCompression(CompressionCodec.NONE)
            .setChunks(this.chunkInfos.subList(0, 1))
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()).setColumnCount(0L));

    ResultData localResultData = new ResultData().setExternalLinks(getChunkLinks(0L, 0L, true));

    setupMockResponse();
    when(mockHttpClient.execute(isA(HttpUriRequest.class), eq(true))).thenReturn(httpResponse);

    ArrowStreamResult result =
        new ArrowStreamResult(
            resultManifest, localResultData, STATEMENT_ID, localSession, mockHttpClient);

    // Verify result was created successfully with RemoteChunkProvider
    assertNotNull(result);
    assertTrue(result.hasNext(), "Result should have data");
    assertTrue(result.next());
    assertDoesNotThrow(result::close);
  }

  @Test
  public void testStreamingChunkProviderEnabledForThriftResult() throws Exception {
    // Enable StreamingChunkProvider via connection property
    Properties props = new Properties();
    props.setProperty("EnableStreamingChunkProvider", "1");
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, props);

    assertTrue(
        connectionContext.isStreamingChunkProviderEnabled(),
        "StreamingChunkProvider should be enabled via property");

    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(metadataResp.getSchema()).thenReturn(TEST_TABLE_SCHEMA);

    // Create result links for cloud fetch path (non-inline)
    TSparkArrowResultLink resultLink =
        new TSparkArrowResultLink()
            .setFileLink("http://test-url/chunk-0")
            .setStartRowOffset(0L)
            .setRowCount(100L)
            .setExpiryTime(
                java.time.Instant.now().plusSeconds(3600).toEpochMilli()); // 1 hour from now
    when(resultData.getResultLinks()).thenReturn(Collections.singletonList(resultLink));
    when(fetchResultsResp.getResults()).thenReturn(resultData);
    when(fetchResultsResp.getResultSetMetadata()).thenReturn(metadataResp);
    when(parentStatement.getStatementId()).thenReturn(STATEMENT_ID);

    setupMockResponse();
    when(mockHttpClient.execute(isA(HttpUriRequest.class), eq(true))).thenReturn(httpResponse);

    ArrowStreamResult result =
        new ArrowStreamResult(fetchResultsResp, parentStatement, session, mockHttpClient);

    // Verify result was created successfully with StreamingChunkProvider for Thrift
    assertNotNull(result);
    assertTrue(result.hasNext(), "Result should have data");
    assertTrue(result.next());
    assertDoesNotThrow(result::close);
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

  // ==================== End of Stream Conversion Tests ====================

  @Test
  public void testSeaEmptyLinksWithZeroChunkCountReturnsEndOfStream() throws Exception {
    // Enable StreamingChunkProvider
    Properties props = new Properties();
    props.setProperty("EnableStreamingChunkProvider", "1");
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, props);

    assertTrue(connectionContext.isStreamingChunkProviderEnabled());

    DatabricksSession localSession = new DatabricksSession(connectionContext, mockedSdkClient);

    // Create result manifest with zero chunks and empty external links
    ResultManifest resultManifest =
        new ResultManifest()
            .setTotalChunkCount(0L)
            .setTotalRowCount(0L)
            .setResultCompression(CompressionCodec.NONE)
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()).setColumnCount(0L));

    ResultData localResultData = new ResultData().setExternalLinks(new ArrayList<>());

    // Should create result successfully with end-of-stream signal
    ArrowStreamResult result =
        new ArrowStreamResult(
            resultManifest, localResultData, STATEMENT_ID, localSession, mockHttpClient);

    assertNotNull(result);
    assertFalse(result.hasNext(), "Empty result should have no data");
    assertDoesNotThrow(result::close);
  }

  @Test
  public void testSeaNullLinksWithZeroChunkCountReturnsEndOfStream() throws Exception {
    // Enable StreamingChunkProvider
    Properties props = new Properties();
    props.setProperty("EnableStreamingChunkProvider", "1");
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, props);

    assertTrue(connectionContext.isStreamingChunkProviderEnabled());

    DatabricksSession localSession = new DatabricksSession(connectionContext, mockedSdkClient);

    // Create result manifest with zero chunks and null external links
    ResultManifest resultManifest =
        new ResultManifest()
            .setTotalChunkCount(0L)
            .setTotalRowCount(0L)
            .setResultCompression(CompressionCodec.NONE)
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()).setColumnCount(0L));

    ResultData localResultData = new ResultData().setExternalLinks(null);

    // Should create result successfully with end-of-stream signal
    ArrowStreamResult result =
        new ArrowStreamResult(
            resultManifest, localResultData, STATEMENT_ID, localSession, mockHttpClient);

    assertNotNull(result);
    assertFalse(result.hasNext(), "Empty result should have no data");
    assertDoesNotThrow(result::close);
  }
}
