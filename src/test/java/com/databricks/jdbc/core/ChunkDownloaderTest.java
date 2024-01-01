package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.core.ArrowResultChunk.DownloadStatus;
import com.databricks.sdk.service.sql.*;
import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
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

  private final Random random = new Random();
  @Mock private IDatabricksSession mockedSession;
  @Mock private ExecutorService mockedChunkDownloaderExecutorService;
  @Mock private IDatabricksHttpClient mockedHttpClient;

  @Mock private ResultData mockedResultData;

  @Mock private ResultManifest mockedResultManifest;

  @Test
  public void testInitEmptyChunkDownloader() {
    when(mockedResultManifest.getTotalChunkCount()).thenReturn(0L);
    assertDoesNotThrow(
        () ->
            new ChunkDownloader(STATEMENT_ID, mockedResultManifest, mockedResultData, null, null));
  }

  @Test
  public void testInitChunkDownloader() throws Exception {
    when(mockedResultManifest.getTotalChunkCount()).thenReturn(TOTAL_CHUNKS);
    when(mockedResultManifest.getChunks()).thenReturn(getChunks());
    when(mockedResultData.getExternalLinks()).thenReturn(getChunkLinks(0, false));
    ChunkDownloader chunkDownloader =
        new ChunkDownloader(
            STATEMENT_ID, mockedResultManifest, mockedResultData, mockedSession, mockedHttpClient);

    assertEquals(4, chunkDownloader.getTotalChunksInMemory());
    assertTrue(chunkDownloader.hasNextChunk());

    for (long chunkResultIndex = 0L; chunkResultIndex < TOTAL_CHUNKS; chunkResultIndex++) {
      assertTrue(chunkDownloader.next());
      assertChunkResult(chunkDownloader.getChunk(), chunkResultIndex);
    }
  }

  private List<BaseChunkInfo> getChunks() {
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
    return chunks;
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

  private Schema createTestSchema() {
    List<Field> fieldList = new ArrayList<>();
    FieldType fieldType1 = new FieldType(false, Types.MinorType.INT.getType(), null);
    FieldType fieldType2 = new FieldType(false, Types.MinorType.FLOAT8.getType(), null);
    fieldList.add(new Field("Field1", fieldType1, null));
    fieldList.add(new Field("Field2", fieldType2, null));
    return new Schema(fieldList);
  }
}
