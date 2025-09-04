package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class JdbcThreadUtilsTest {

  @Mock private IDatabricksConnectionContext mockConnectionContext;

  private MockedStatic<DatabricksThreadContextHolder> mockedContextHolder;

  @BeforeEach
  public void setUp() {
    mockedContextHolder = mockStatic(DatabricksThreadContextHolder.class);
  }

  @AfterEach
  public void tearDown() {
    if (mockedContextHolder != null) {
      mockedContextHolder.close();
    }
  }

  @Test
  public void testParallelExecuteWithEmptyCollection() throws SQLException {
    List<String> result =
        JdbcThreadUtils.parallelMap(
            Collections.<String>emptyList(),
            mockConnectionContext,
            5,
            10,
            String::toUpperCase,
            null);

    assertTrue(result.isEmpty());

    // Verify that thread context methods are not called
    mockedContextHolder.verify(
        () -> DatabricksThreadContextHolder.setConnectionContext(any()), never());
    mockedContextHolder.verify(DatabricksThreadContextHolder::clearConnectionContext, never());
  }

  @Test
  public void testParallelExecuteWithSingleItem() throws SQLException {
    List<String> items = Collections.singletonList("test");

    List<String> result =
        JdbcThreadUtils.parallelMap(items, mockConnectionContext, 5, 10, String::toUpperCase, null);

    assertEquals(1, result.size());
    assertEquals("TEST", result.get(0));
  }

  @Test
  public void testParallelExecuteWithMultipleItems() throws SQLException {
    List<String> items = Arrays.asList("test1", "test2", "test3");

    List<String> result =
        JdbcThreadUtils.parallelMap(items, mockConnectionContext, 5, 10, String::toUpperCase, null);

    assertEquals(3, result.size());
    assertTrue(result.contains("TEST1"));
    assertTrue(result.contains("TEST2"));
    assertTrue(result.contains("TEST3"));
  }

  @Test
  public void testParallelExecuteWithException() {
    List<String> items = Arrays.asList("test1", "error", "test3");

    final SQLException sqlException = new SQLException("Test SQL exception");

    Exception exception =
        assertThrows(
            SQLException.class,
            () ->
                JdbcThreadUtils.parallelMap(
                    items,
                    mockConnectionContext,
                    5,
                    10,
                    item -> {
                      if ("error".equals(item)) {
                        throw new RuntimeException(sqlException);
                      }
                      return item.toUpperCase();
                    },
                    null));

    // Check if the cause is our original SQLException
    assertNotNull(exception);
    assertEquals("Test SQL exception", exception.getMessage());
  }

  @Test
  @Timeout(3) // Test should complete in under 3 seconds
  public void testParallelExecuteWithTimeout() {
    List<String> items = Arrays.asList("test1", "test2", "test3");

    // Set a very short timeout that will be exceeded
    final int timeoutSeconds = 1;

    Exception exception =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                JdbcThreadUtils.parallelMap(
                    items,
                    mockConnectionContext,
                    5,
                    timeoutSeconds,
                    item -> {
                      try {
                        // Sleep longer than the timeout
                        Thread.sleep(timeoutSeconds * 2000);
                      } catch (InterruptedException e) {
                        // Expected
                      }
                      return item.toUpperCase();
                    },
                    null));

    assertTrue(exception.getMessage().contains("timed out"));
  }

  @Test
  public void testParallelFlatExecuteWithMultipleItems() throws SQLException {
    List<String> items = Arrays.asList("test1", "test2", "test3");

    List<String> result =
        JdbcThreadUtils.parallelFlatMap(
            items,
            mockConnectionContext,
            5,
            10,
            item -> Arrays.asList(item.toUpperCase(), item.toLowerCase()),
            null);

    assertEquals(6, result.size());
    assertTrue(result.contains("TEST1"));
    assertTrue(result.contains("test1"));
    assertTrue(result.contains("TEST2"));
    assertTrue(result.contains("test2"));
    assertTrue(result.contains("TEST3"));
    assertTrue(result.contains("test3"));
  }

  @Test
  public void testParallelFlatExecuteWithNullCollection() throws SQLException {
    List<String> items = Arrays.asList("test1", "test2", "null");

    List<String> result =
        JdbcThreadUtils.parallelFlatMap(
            items,
            mockConnectionContext,
            5,
            10,
            item -> {
              if ("null".equals(item)) {
                return null;
              }
              return Collections.singletonList(item.toUpperCase());
            },
            null);

    assertEquals(2, result.size());
    assertTrue(result.contains("TEST1"));
    assertTrue(result.contains("TEST2"));
  }

  @Test
  public void testInterruptionHandling() throws InterruptedException {
    List<String> items = Collections.singletonList("test");

    Thread testThread =
        new Thread(
            () -> {
              try {
                JdbcThreadUtils.parallelMap(
                    items,
                    mockConnectionContext,
                    1,
                    10,
                    item -> {
                      try {
                        Thread.sleep(5000);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                      return item;
                    },
                    null);
              } catch (SQLException e) {
                // Expected
              }
            });

    testThread.start();
    Thread.sleep(100); // Give the thread time to start
    testThread.interrupt();
    testThread.join(2000); // Wait for thread to finish

    assertFalse(testThread.isAlive(), "Thread should have terminated");
  }
}
