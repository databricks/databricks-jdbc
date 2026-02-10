package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ExternalLink;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for SeaChunkLinkFetcher and ThriftChunkLinkFetcher.
 *
 * <p>Both implementations share similar structure but differ in: - SEA uses chunkIndex for
 * continuation - Thrift uses rowOffset for FETCH_ABSOLUTE continuation - Thrift has retry logic in
 * refetchLink when hasMore=true but no links returned
 */
@ExtendWith(MockitoExtension.class)
public class ChunkLinkFetcherTest {

  private static final StatementId STATEMENT_ID = new StatementId("test-statement-id");

  @Mock private IDatabricksSession mockSession;
  @Mock private IDatabricksClient mockClient;

  @BeforeEach
  void setUp() {
    lenient().when(mockSession.getDatabricksClient()).thenReturn(mockClient);
  }

  private ExternalLink createLink(long chunkIndex, long rowOffset, long rowCount) {
    ExternalLink link = new ExternalLink();
    link.setChunkIndex(chunkIndex);
    link.setRowOffset(rowOffset);
    link.setRowCount(rowCount);
    link.setExternalLink("http://test-url/chunk-" + chunkIndex);
    link.setHttpHeaders(Collections.emptyMap());
    return link;
  }

  // ==================== SeaChunkLinkFetcher Tests ====================

  @Nested
  @DisplayName("SeaChunkLinkFetcher")
  class SeaChunkLinkFetcherTests {

    private SeaChunkLinkFetcher fetcher;

    @BeforeEach
    void setUp() {
      fetcher = new SeaChunkLinkFetcher(mockSession, STATEMENT_ID);
    }

    @Test
    @DisplayName("fetchLinks should delegate to client with chunkIndex")
    void testFetchLinksDelegatesToClient() throws SQLException {
      ExternalLink link = createLink(5, 500, 100);
      ChunkLinkFetchResult expectedResult = ChunkLinkFetchResult.of(List.of(link), true, 6, 600);

      when(mockClient.getResultChunks(STATEMENT_ID, 5L, 500L)).thenReturn(expectedResult);

      ChunkLinkFetchResult result = fetcher.fetchLinks(5L, 500L);

      assertSame(expectedResult, result);
      verify(mockClient).getResultChunks(STATEMENT_ID, 5L, 500L);
    }

    @Test
    @DisplayName("refetchLink should return matching link from result")
    void testRefetchLinkReturnsMatchingLink() throws SQLException {
      ExternalLink link5 = createLink(5, 500, 100);
      ExternalLink link6 = createLink(6, 600, 100);
      ChunkLinkFetchResult result = ChunkLinkFetchResult.of(List.of(link5, link6), true, 7, 700);

      when(mockClient.getResultChunks(STATEMENT_ID, 5L, 500L)).thenReturn(result);

      ExternalLink refetched = fetcher.refetchLink(5L, 500L);

      assertSame(link5, refetched);
    }

    @Test
    @DisplayName("refetchLink should throw when result is end of stream")
    void testRefetchLinkThrowsOnEndOfStream() throws SQLException {
      when(mockClient.getResultChunks(STATEMENT_ID, 5L, 500L))
          .thenReturn(ChunkLinkFetchResult.endOfStream());

      DatabricksSQLException exception =
          assertThrows(DatabricksSQLException.class, () -> fetcher.refetchLink(5L, 500L));

      assertTrue(exception.getMessage().contains("no links returned"));
    }

    @Test
    @DisplayName("refetchLink should throw when no matching link found")
    void testRefetchLinkThrowsWhenNoMatch() throws SQLException {
      // Return links for chunk 6 and 7, but we're looking for chunk 5
      ExternalLink link6 = createLink(6, 600, 100);
      ExternalLink link7 = createLink(7, 700, 100);
      ChunkLinkFetchResult result = ChunkLinkFetchResult.of(List.of(link6, link7), true, 8, 800);

      when(mockClient.getResultChunks(STATEMENT_ID, 5L, 500L)).thenReturn(result);

      DatabricksSQLException exception =
          assertThrows(DatabricksSQLException.class, () -> fetcher.refetchLink(5L, 500L));

      assertTrue(exception.getMessage().contains("none matched requested index"));
    }

    @Test
    @DisplayName("close should not throw")
    void testCloseDoesNotThrow() {
      assertDoesNotThrow(() -> fetcher.close());
    }
  }

  // ==================== ThriftChunkLinkFetcher Tests ====================

  @Nested
  @DisplayName("ThriftChunkLinkFetcher")
  class ThriftChunkLinkFetcherTests {

    private ThriftChunkLinkFetcher fetcher;

    @BeforeEach
    void setUp() {
      fetcher = new ThriftChunkLinkFetcher(mockSession, STATEMENT_ID);
    }

    @Test
    @DisplayName("fetchLinks should delegate to client with rowOffset")
    void testFetchLinksDelegatesToClient() throws SQLException {
      ExternalLink link = createLink(5, 500, 100);
      ChunkLinkFetchResult expectedResult = ChunkLinkFetchResult.of(List.of(link), true, 6, 600);

      when(mockClient.getResultChunks(STATEMENT_ID, 5L, 500L)).thenReturn(expectedResult);

      ChunkLinkFetchResult result = fetcher.fetchLinks(5L, 500L);

      assertSame(expectedResult, result);
      verify(mockClient).getResultChunks(STATEMENT_ID, 5L, 500L);
    }

    @Test
    @DisplayName("refetchLink should return matching link from result")
    void testRefetchLinkReturnsMatchingLink() throws SQLException {
      ExternalLink link5 = createLink(5, 500, 100);
      ExternalLink link6 = createLink(6, 600, 100);
      ChunkLinkFetchResult result = ChunkLinkFetchResult.of(List.of(link5, link6), true, 7, 700);

      when(mockClient.getResultChunks(STATEMENT_ID, 5L, 500L)).thenReturn(result);

      ExternalLink refetched = fetcher.refetchLink(5L, 500L);

      assertSame(link5, refetched);
    }

    @Test
    @DisplayName("refetchLink should retry when hasMore=true but no links")
    void testRefetchLinkRetriesWhenHasMoreButNoLinks() throws SQLException {
      // First two calls return empty with hasMore=true, third call returns the link
      ChunkLinkFetchResult emptyWithMore =
          ChunkLinkFetchResult.of(Collections.emptyList(), true, 5, 500);
      ExternalLink link5 = createLink(5, 500, 100);
      ChunkLinkFetchResult successResult = ChunkLinkFetchResult.of(List.of(link5), false, -1, 600);

      when(mockClient.getResultChunks(STATEMENT_ID, 5L, 500L))
          .thenReturn(emptyWithMore)
          .thenReturn(emptyWithMore)
          .thenReturn(successResult);

      ExternalLink refetched = fetcher.refetchLink(5L, 500L);

      assertSame(link5, refetched);
      verify(mockClient, times(3)).getResultChunks(STATEMENT_ID, 5L, 500L);
    }

    @Test
    @DisplayName("refetchLink should throw when hasMore=false and no links")
    void testRefetchLinkThrowsWhenNoMoreAndNoLinks() throws SQLException {
      ChunkLinkFetchResult emptyNoMore =
          ChunkLinkFetchResult.of(Collections.emptyList(), false, -1, 500);

      when(mockClient.getResultChunks(STATEMENT_ID, 5L, 500L)).thenReturn(emptyNoMore);

      DatabricksSQLException exception =
          assertThrows(DatabricksSQLException.class, () -> fetcher.refetchLink(5L, 500L));

      assertTrue(exception.getMessage().contains("no links returned and hasMore=false"));
      // Should not retry when hasMore=false
      verify(mockClient, times(1)).getResultChunks(STATEMENT_ID, 5L, 500L);
    }

    @Test
    @DisplayName("refetchLink should throw when no matching link found")
    void testRefetchLinkThrowsWhenNoMatch() throws SQLException {
      // Return links for chunk 6 and 7, but we're looking for chunk 5
      ExternalLink link6 = createLink(6, 600, 100);
      ExternalLink link7 = createLink(7, 700, 100);
      ChunkLinkFetchResult result = ChunkLinkFetchResult.of(List.of(link6, link7), true, 8, 800);

      when(mockClient.getResultChunks(STATEMENT_ID, 5L, 500L)).thenReturn(result);

      DatabricksSQLException exception =
          assertThrows(DatabricksSQLException.class, () -> fetcher.refetchLink(5L, 500L));

      assertTrue(exception.getMessage().contains("none matched requested index"));
    }

    @Test
    @DisplayName("close should not throw")
    void testCloseDoesNotThrow() {
      assertDoesNotThrow(() -> fetcher.close());
    }
  }
}
