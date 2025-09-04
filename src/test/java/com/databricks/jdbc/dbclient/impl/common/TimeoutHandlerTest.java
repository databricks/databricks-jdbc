package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TimeoutHandlerTest {

  @Mock private IDatabricksClient mockClient;

  @Mock private Runnable mockTimeoutAction;

  @Mock private StatementId mockStatementId;

  @Test
  void testNoTimeout() {
    // Create handler with no timeout (0 seconds)
    TimeoutHandler handler = new TimeoutHandler(0, "Test operation", mockTimeoutAction);

    // This should not throw an exception
    assertDoesNotThrow(handler::checkTimeout);

    // Verify timeout action was not called
    verifyNoInteractions(mockTimeoutAction);
  }

  @Test
  void testTimeoutNotReached() {
    // Create handler with 10 second timeout
    TimeoutHandler handler = new TimeoutHandler(10, "Test operation", mockTimeoutAction);

    // This should not throw an exception
    assertDoesNotThrow(handler::checkTimeout);

    // Verify timeout action was not called
    verifyNoInteractions(mockTimeoutAction);
  }

  @Test
  void testTimeoutActionExecuted() throws Exception {
    // Create handler with 2 second timeout
    TimeoutHandler handler = new TimeoutHandler(2, "Test operation", mockTimeoutAction);

    // Manipulate the startTimeMillis to simulate elapsed time
    Field startTimeField = TimeoutHandler.class.getDeclaredField("startTimeMillis");
    startTimeField.setAccessible(true);
    long currentTime = System.currentTimeMillis();
    startTimeField.set(handler, currentTime - TimeUnit.SECONDS.toMillis(3)); // 3 seconds ago

    // This should throw a DatabricksTimeoutException
    DatabricksTimeoutException exception =
        assertThrows(DatabricksTimeoutException.class, handler::checkTimeout);

    // Verify timeout action was called
    verify(mockTimeoutAction, times(1)).run();

    // Verify exception message
    assertTrue(exception.getMessage().contains("timed-out after 2 seconds"));
    assertTrue(exception.getMessage().contains("Test operation"));
  }

  @Test
  void testTimeoutActionThrowsException() throws Exception {
    // Configure mock to throw exception
    doThrow(new RuntimeException("Test exception")).when(mockTimeoutAction).run();

    // Create handler with 2 second timeout
    TimeoutHandler handler = new TimeoutHandler(2, "Test operation", mockTimeoutAction);

    // Manipulate the startTimeMillis to simulate elapsed time
    Field startTimeField = TimeoutHandler.class.getDeclaredField("startTimeMillis");
    startTimeField.setAccessible(true);
    long currentTime = System.currentTimeMillis();
    startTimeField.set(handler, currentTime - TimeUnit.SECONDS.toMillis(3)); // 3 seconds ago

    // This should still throw a DatabricksTimeoutException even if action throws
    DatabricksTimeoutException exception =
        assertThrows(DatabricksTimeoutException.class, handler::checkTimeout);

    // Verify timeout action was called
    verify(mockTimeoutAction, times(1)).run();

    // Verify DatabricksTimeoutException is still thrown
    assertTrue(exception.getMessage().contains("timed-out after 2 seconds"));
  }

  @Test
  void testNullTimeoutAction() throws Exception {
    // Create handler with null timeout action
    TimeoutHandler handler = new TimeoutHandler(2, "Test operation", null);

    // Manipulate the startTimeMillis to simulate elapsed time
    Field startTimeField = TimeoutHandler.class.getDeclaredField("startTimeMillis");
    startTimeField.setAccessible(true);
    long currentTime = System.currentTimeMillis();
    startTimeField.set(handler, currentTime - TimeUnit.SECONDS.toMillis(3)); // 3 seconds ago

    // This should throw a DatabricksTimeoutException
    DatabricksTimeoutException exception =
        assertThrows(DatabricksTimeoutException.class, handler::checkTimeout);

    // Verify exception message
    assertTrue(exception.getMessage().contains("timed-out after 2 seconds"));
  }

  @Test
  void testForStatementFactory() throws Exception {
    when(mockStatementId.toString()).thenReturn("test-statement-id");

    // Create handler with factory method
    TimeoutHandler handler = TimeoutHandler.forStatement(5, mockStatementId, mockClient);

    // Verify handler was created correctly
    Field timeoutSecondsField = TimeoutHandler.class.getDeclaredField("timeoutSeconds");
    timeoutSecondsField.setAccessible(true);
    assertEquals(5, timeoutSecondsField.getInt(handler));

    Field operationDescriptionField = TimeoutHandler.class.getDeclaredField("operationDescription");
    operationDescriptionField.setAccessible(true);
    assertEquals("Statement ID: test-statement-id", operationDescriptionField.get(handler));

    // Manipulate the startTimeMillis to simulate elapsed time
    Field startTimeField = TimeoutHandler.class.getDeclaredField("startTimeMillis");
    startTimeField.setAccessible(true);
    long currentTime = System.currentTimeMillis();
    startTimeField.set(handler, currentTime - TimeUnit.SECONDS.toMillis(6)); // 6 seconds ago

    // This should throw a DatabricksTimeoutException
    assertThrows(DatabricksTimeoutException.class, handler::checkTimeout);

    // Verify client.cancelStatement was called
    verify(mockClient, times(1)).cancelStatement(mockStatementId);
  }

  @Test
  void testForStatementCancelFailure() throws Exception {
    when(mockStatementId.toString()).thenReturn("test-statement-id");
    // Configure mock to throw exception
    doThrow(new RuntimeException("Cancel failed"))
        .when(mockClient)
        .cancelStatement(mockStatementId);

    // Create handler with factory method
    TimeoutHandler handler = TimeoutHandler.forStatement(5, mockStatementId, mockClient);

    // Manipulate the startTimeMillis to simulate elapsed time
    Field startTimeField = TimeoutHandler.class.getDeclaredField("startTimeMillis");
    startTimeField.setAccessible(true);
    long currentTime = System.currentTimeMillis();
    startTimeField.set(handler, currentTime - TimeUnit.SECONDS.toMillis(6)); // 6 seconds ago

    // This should throw a DatabricksTimeoutException
    DatabricksTimeoutException exception =
        assertThrows(DatabricksTimeoutException.class, handler::checkTimeout);

    // Verify client.cancelStatement was called
    verify(mockClient, times(1)).cancelStatement(mockStatementId);

    // Verify DatabricksTimeoutException is still thrown
    assertTrue(exception.getMessage().contains("timed-out after 5 seconds"));
  }
}
