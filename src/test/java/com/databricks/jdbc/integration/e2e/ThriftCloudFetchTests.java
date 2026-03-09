package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.getValidJDBCConnection;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.impl.DatabricksStatement;
import com.databricks.jdbc.api.impl.arrow.AbstractRemoteChunkProvider;
import com.databricks.jdbc.api.impl.arrow.ArrowResultChunk;
import com.databricks.jdbc.api.impl.arrow.ChunkProvider;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ExternalLink;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration test to test CloudFetch link refetching using Thrift client. */
public class ThriftCloudFetchTests {
  /** Connection to the Databricks cluster. */
  private Connection connection;

  /** Table with lot of rows to generate multiple CloudFetch chunks. */
  private static final String TABLE = "samples.tpch.lineitem";

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ThriftCloudFetchTests.class);

  @BeforeEach
  void setUp() throws Exception {
    Properties props = new Properties();
    props.put("UseThriftClient", "1"); // Create connection with Thrift client enabled
    props.put("EnableDirectResults", "0"); // Disable direct results to test CloudFetch
    props.put("CloudFetchThreadPoolSize", "1"); // Download only a small chunk.

    connection = getValidJDBCConnection(props);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  /**
   * Test refetching CloudFetch links from various startRowOffsets.
   *
   * <p>This test:
   *
   * <ol>
   *   <li>Executes a query that generates multiple CloudFetch chunks
   *   <li>Extracts the chunk provider to get chunk metadata
   *   <li>Refetches from 3 different offsets: start (0), middle, and end
   *   <li>Verifies each refetch returns correct subset of links
   *   <li>Verifies link properties match (chunkIndex, rowOffset, rowCount, byteCount)
   * </ol>
   */
  @Test
  void testCloudFetchLinksRefetchAtStartRowOffset() throws Exception {
    // Step 1: Execute a query that returns a large dataset with multiple CloudFetch chunks
    int maxRows = 6_000_000; // Generate many chunk links.
    String query = "SELECT * FROM " + TABLE + " LIMIT " + maxRows;

    try (Statement stmt = connection.createStatement()) {
      stmt.execute(query);
      ResultSet rs = stmt.getResultSet();

      LOGGER.info("Query executed, extracting chunks...");

      // Step 2: Extract the chunks that were created by initializeChunksMap
      DatabricksStatement dbStatement = (DatabricksStatement) stmt;
      StatementId statementId = dbStatement.getStatementId();
      assertNotNull(statementId, "StatementId should be set after execution");

      Optional<ChunkProvider> chunkProviderOptional = ((DatabricksResultSet) rs).getChunkProvider();
      assertTrue(
          chunkProviderOptional.isPresent(),
          "Chunk provider should exist for CloudFetch result set");
      @SuppressWarnings("unchecked")
      AbstractRemoteChunkProvider<ArrowResultChunk> chunkProvider =
          (AbstractRemoteChunkProvider<ArrowResultChunk>) chunkProviderOptional.get();

      long totalChunks = chunkProvider.getChunkCount();

      assertTrue(
          totalChunks > 2, "Should have at least 3 chunks for this test, got: " + totalChunks);
      LOGGER.info("Total chunks: " + totalChunks);

      // Step 3: Test refetching from various chunk indices.
      DatabricksConnection dbConnection =
          (DatabricksConnection) connection.unwrap(DatabricksConnection.class);
      IDatabricksSession session = dbConnection.getSession();
      IDatabricksClient client = session.getDatabricksClient();

      long end = totalChunks - 1;
      long mid = totalChunks / 2;
      List<Long> chunkIndicesToTest = new ArrayList<>(List.of(0L, 1L, mid - 1, mid, mid + 1, end));
      // Randomize the order to make sure there is no sequence to the responses. We use FETCH_NEXT
      // when fetching the next set of links.
      Collections.shuffle(chunkIndicesToTest);

      for (long chunkIndex : chunkIndicesToTest) {
        ArrowResultChunk targetChunk = chunkProvider.getChunkByIndex(chunkIndex);
        assertNotNull(targetChunk, "Target chunk should exist at index " + chunkIndex);

        long chunkStartRowOffset = targetChunk.getStartRowOffset();
        LOGGER.info(
            "Refetching from chunk index "
                + chunkIndex
                + " with startRowOffset: "
                + chunkStartRowOffset);

        testRefetchLinks(statementId, chunkIndex, chunkStartRowOffset, chunkProvider, client);
      }
    }
  }

  private void testRefetchLinks(
      StatementId statementId,
      long chunkIndex,
      long chunkStartRowOffset,
      AbstractRemoteChunkProvider<ArrowResultChunk> chunkProvider,
      IDatabricksClient client)
      throws SQLException {
    // Fetch from the startRowOffset of the target chunk.
    Collection<ExternalLink> refetchedLinks =
        client.getResultChunks(statementId, chunkIndex, chunkStartRowOffset).getChunkLinks();

    assertNotNull(refetchedLinks, "Refetched links should not be null");
    assertFalse(refetchedLinks.isEmpty(), "Refetched links should not be empty");

    LOGGER.info("Refetched " + refetchedLinks.size() + " links from chunk index " + chunkIndex);

    // Convert refetched links to a list for comparison
    List<ExternalLink> refetchedLinksList = new ArrayList<>(refetchedLinks);

    // Compare each refetched link with the corresponding original link.
    for (int i = 0; i < refetchedLinksList.size(); i++) {
      long originalChunkIndex = chunkIndex + i;
      ArrowResultChunk originalChunk = chunkProvider.getChunkByIndex(originalChunkIndex);
      assertNotNull(originalChunk, "Original chunk should exist at index " + originalChunkIndex);

      ExternalLink originalLink = originalChunk.getChunkLink();
      ExternalLink refetchedLink = refetchedLinksList.get(i);

      assertEquals(
          originalLink.getChunkIndex(),
          refetchedLink.getChunkIndex(),
          "Chunk index should match for chunk " + originalChunkIndex);

      assertEquals(
          originalLink.getRowOffset(),
          refetchedLink.getRowOffset(),
          "Start row offset should match for chunk " + originalChunkIndex);

      assertEquals(
          originalLink.getRowCount(),
          refetchedLink.getRowCount(),
          "Row count should match for chunk " + originalChunkIndex);

      assertEquals(
          originalLink.getByteCount(),
          refetchedLink.getByteCount(),
          "Byte count should match for chunk " + originalChunkIndex);

      assertNotNull(originalLink.getExternalLink(), "Original file link should not be null");
      assertNotNull(refetchedLink.getExternalLink(), "Refetched file link should not be null");
    }
  }
}
