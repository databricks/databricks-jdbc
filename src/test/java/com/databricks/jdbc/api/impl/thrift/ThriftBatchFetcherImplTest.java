package com.databricks.jdbc.api.impl.thrift;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Unit tests for ThriftBatchFetcherImpl. */
@ExtendWith(MockitoExtension.class)
public class ThriftBatchFetcherImplTest {

  @Mock private IDatabricksSession session;
  @Mock private IDatabricksStatementInternal statement;
  @Mock private IDatabricksClient databricksClient;

  @BeforeEach
  void setUp() {
    lenient().when(session.getDatabricksClient()).thenReturn(databricksClient);
  }

  @Test
  void testFetchNextBatchWhenClosed() throws DatabricksSQLException {
    ThriftBatchFetcherImpl fetcher = new ThriftBatchFetcherImpl(session, statement);

    // Close the fetcher
    fetcher.close();

    // Attempting to fetch should throw exception
    DatabricksSQLException thrown =
        assertThrows(DatabricksSQLException.class, fetcher::fetchNextBatch);
    assertTrue(thrown.getMessage().contains("closed"));
  }

  @Test
  void testCloseAndIsClosed() {
    ThriftBatchFetcherImpl fetcher = new ThriftBatchFetcherImpl(session, statement);

    // Initially not closed
    assertFalse(fetcher.isClosed());

    // Close it
    fetcher.close();

    // Now should be closed
    assertTrue(fetcher.isClosed());

    // Closing again should be safe (idempotent)
    assertDoesNotThrow(fetcher::close);
    assertTrue(fetcher.isClosed());
  }
}
