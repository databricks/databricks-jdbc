package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ArrowResultChunkStateMachineTest {
  private static final long CHUNK_INDEX = 1L;
  private static final StatementId STATEMENT_ID = new StatementId("test-statement-id");
  private ArrowResultChunkStateMachine stateMachine;

  @BeforeEach
  void setUp() {
    stateMachine = new ArrowResultChunkStateMachine(ChunkStatus.PENDING, CHUNK_INDEX, STATEMENT_ID);
  }

  @Test
  @DisplayName("Initial state should be PENDING")
  void testInitialState() {
    assertEquals(ChunkStatus.PENDING, stateMachine.getCurrentStatus());
  }

  @Test
  @DisplayName("Valid transition from PENDING to URL_FETCHED")
  void testValidTransitionFromPending() throws DatabricksParsingException {
    stateMachine.transition(ChunkStatus.URL_FETCHED);
    assertEquals(ChunkStatus.URL_FETCHED, stateMachine.getCurrentStatus());
  }

  @Test
  @DisplayName("Same state transition should not throw exception")
  void testSameStateTransition() {
    assertDoesNotThrow(() -> stateMachine.transition(ChunkStatus.PENDING));
    assertEquals(ChunkStatus.PENDING, stateMachine.getCurrentStatus());
  }

  @Test
  @DisplayName("Invalid transition should throw DatabricksParsingException")
  void testInvalidTransition() {
    DatabricksParsingException exception =
        assertThrows(
            DatabricksParsingException.class,
            () -> stateMachine.transition(ChunkStatus.PROCESSING_SUCCEEDED));
    assertTrue(exception.getMessage().contains("Invalid state transition"));
  }

  @Test
  @DisplayName("Test transition to terminal CHUNK_RELEASED state")
  void testTransitionToTerminalState() throws DatabricksParsingException {
    stateMachine.transition(ChunkStatus.CHUNK_RELEASED);
    assertEquals(ChunkStatus.CHUNK_RELEASED, stateMachine.getCurrentStatus());
    assertTrue(stateMachine.getValidTargetStates().isEmpty());
  }

  @Test
  @DisplayName("Test isValidTransition method")
  void testIsValidTransition() {
    assertTrue(stateMachine.isValidTransition(ChunkStatus.URL_FETCHED));
    assertTrue(stateMachine.isValidTransition(ChunkStatus.CHUNK_RELEASED));
    assertFalse(stateMachine.isValidTransition(ChunkStatus.PROCESSING_SUCCEEDED));
  }

  @Test
  @DisplayName("Test getValidTargetStates method")
  void testGetValidTargetStates() {
    Set<ChunkStatus> validStates = stateMachine.getValidTargetStates();
    assertEquals(3, validStates.size());
    assertTrue(validStates.contains(ChunkStatus.URL_FETCHED));
    assertTrue(validStates.contains(ChunkStatus.CHUNK_RELEASED));
    assertTrue(validStates.contains(ChunkStatus.DOWNLOAD_FAILED));
  }

  // Test full path scenarios
  @Test
  @DisplayName("Test successful download and processing path")
  void testSuccessfulPath() throws DatabricksParsingException {
    // PENDING -> URL_FETCHED -> DOWNLOAD_SUCCEEDED -> PROCESSING_SUCCEEDED -> CHUNK_RELEASED
    stateMachine.transition(ChunkStatus.URL_FETCHED);
    stateMachine.transition(ChunkStatus.DOWNLOAD_SUCCEEDED);
    stateMachine.transition(ChunkStatus.PROCESSING_SUCCEEDED);
    stateMachine.transition(ChunkStatus.CHUNK_RELEASED);
    assertEquals(ChunkStatus.CHUNK_RELEASED, stateMachine.getCurrentStatus());
  }

  @Test
  @DisplayName("Test failed download with retry path")
  void testFailedDownloadWithRetryPath() throws DatabricksParsingException {
    // PENDING -> URL_FETCHED -> DOWNLOAD_FAILED -> DOWNLOAD_RETRY -> URL_FETCHED
    stateMachine.transition(ChunkStatus.URL_FETCHED);
    stateMachine.transition(ChunkStatus.DOWNLOAD_FAILED);
    stateMachine.transition(ChunkStatus.DOWNLOAD_RETRY);
    stateMachine.transition(ChunkStatus.URL_FETCHED);
    assertEquals(ChunkStatus.URL_FETCHED, stateMachine.getCurrentStatus());
  }

  @ParameterizedTest
  @MethodSource("validTransitionProvider")
  @DisplayName("Test valid transitions using parameterized test")
  void testValidTransitions(ChunkStatus initialState, ChunkStatus targetState) {
    ArrowResultChunkStateMachine machine =
        new ArrowResultChunkStateMachine(initialState, CHUNK_INDEX, STATEMENT_ID);
    assertDoesNotThrow(() -> machine.transition(targetState));
    assertEquals(targetState, machine.getCurrentStatus());
  }

  // Parameterized test for valid transitions
  private static Stream<Arguments> validTransitionProvider() {
    return Stream.of(
        Arguments.of(ChunkStatus.PENDING, ChunkStatus.URL_FETCHED),
        Arguments.of(ChunkStatus.URL_FETCHED, ChunkStatus.DOWNLOAD_SUCCEEDED),
        Arguments.of(ChunkStatus.URL_FETCHED, ChunkStatus.DOWNLOAD_FAILED),
        Arguments.of(ChunkStatus.URL_FETCHED, ChunkStatus.CANCELLED),
        Arguments.of(ChunkStatus.DOWNLOAD_SUCCEEDED, ChunkStatus.PROCESSING_SUCCEEDED),
        Arguments.of(ChunkStatus.DOWNLOAD_FAILED, ChunkStatus.DOWNLOAD_RETRY));
  }
}
