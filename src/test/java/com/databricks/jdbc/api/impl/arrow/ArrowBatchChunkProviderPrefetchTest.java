package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ArrowBatch;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.Format;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.Channels;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;
import org.junit.jupiter.api.Test;

class ArrowBatchChunkProviderPrefetchTest {
  private static final StatementId STATEMENT_ID =
      new StatementId("01f0aaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

  @Test
  void timesOutConsumerAndStopsBlockedPrefetchWorkerOnClose() throws Exception {
    CountDownLatch fetchStarted = new CountDownLatch(1);
    CountDownLatch releaseFetch = new CountDownLatch(1);
    ArrowBatchChunkProvider provider =
        provider(blockingClient(fetchStarted, releaseFetch), 2, 1);

    try {
      assertTrue(fetchStarted.await(5, TimeUnit.SECONDS), "prefetch did not start");
      assertTrue(provider.next());

      DatabricksSQLException error = assertThrows(DatabricksSQLException.class, provider::next);
      assertEquals(DatabricksDriverErrorCode.CHUNK_READY_ERROR.toString(), error.getSQLState());

      Thread worker = worker(provider);
      assertTrue(worker.isAlive(), "blocked prefetch worker should still be running");
      provider.close();
      assertFalse(worker.isAlive(), "close should terminate the prefetch worker");
    } finally {
      releaseFetch.countDown();
      provider.close();
    }
  }

  @Test
  void interruptedConsumerRestoresInterruptAndCloseStopsWorker() throws Exception {
    CountDownLatch fetchStarted = new CountDownLatch(1);
    CountDownLatch releaseFetch = new CountDownLatch(1);
    ArrowBatchChunkProvider provider =
        provider(blockingClient(fetchStarted, releaseFetch), 2, 0);
    AtomicReference<Throwable> consumerFailure = new AtomicReference<>();
    AtomicBoolean interruptRestored = new AtomicBoolean();

    try {
      assertTrue(fetchStarted.await(5, TimeUnit.SECONDS), "prefetch did not start");
      assertTrue(provider.next());

      Thread consumer =
          new Thread(
              () -> {
                try {
                  provider.next();
                } catch (Throwable error) {
                  consumerFailure.set(error);
                  interruptRestored.set(Thread.currentThread().isInterrupted());
                }
              },
              "apc-prefetch-test-consumer");
      consumer.start();
      awaitThreadState(consumer, Thread.State.WAITING);
      consumer.interrupt();
      consumer.join(5000);

      assertFalse(consumer.isAlive(), "interrupted consumer should terminate");
      assertNotNull(consumerFailure.get(), "consumer should report interruption");
      assertTrue(consumerFailure.get() instanceof DatabricksSQLException);
      assertEquals(
          DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR.toString(),
          ((DatabricksSQLException) consumerFailure.get()).getSQLState());
      assertTrue(interruptRestored.get(), "consumer interrupt flag should be restored");

      Thread worker = worker(provider);
      provider.close();
      assertFalse(worker.isAlive(), "close should terminate the prefetch worker");
    } finally {
      releaseFetch.countDown();
      provider.close();
    }
  }

  @Test
  void closeStopsBackpressuredWorkerAndReleasesBufferedPages() throws Exception {
    IDatabricksClient client = mock(IDatabricksClient.class);
    CountDownLatch thirdPageRequested = new CountDownLatch(1);
    when(client.getResultChunksData(STATEMENT_ID, 1L, 1L)).thenReturn(page(2, 1, 1, 2L));
    when(client.getResultChunksData(STATEMENT_ID, 2L, 2L))
        .thenAnswer(
            ignored -> {
              thirdPageRequested.countDown();
              return page(3, 2, 2, 3L);
            });

    ArrowBatchChunkProvider provider = provider(client, 3, 5);
    try {
      assertTrue(
          thirdPageRequested.await(5, TimeUnit.SECONDS), "third page prefetch did not start");
      Thread worker = worker(provider);
      awaitThreadState(worker, Thread.State.WAITING);

      List<ArrowResultChunk> bufferedChunks = bufferedChunks(provider);
      assertEquals(3, bufferedChunks.size());
      provider.close();

      assertFalse(worker.isAlive(), "close should terminate the backpressured prefetch worker");
      for (ArrowResultChunk chunk : bufferedChunks) {
        assertEquals(ChunkStatus.CHUNK_RELEASED, chunk.getStatus());
      }
      assertTrue(bufferedChunks(provider).isEmpty(), "close should clear all buffered pages");
    } finally {
      provider.close();
    }
  }

  private static ArrowBatchChunkProvider provider(
      IDatabricksClient client, int maxChunksInMemory, int timeoutSeconds) throws Exception {
    return new ArrowBatchChunkProvider(
        page(1, 0, 0, 1L),
        manifest(),
        STATEMENT_ID,
        session(client),
        maxChunksInMemory,
        timeoutSeconds);
  }

  private static IDatabricksClient blockingClient(
      CountDownLatch fetchStarted, CountDownLatch releaseFetch) throws Exception {
    IDatabricksClient client = mock(IDatabricksClient.class);
    when(client.getResultChunksData(STATEMENT_ID, 1L, 1L))
        .thenAnswer(
            ignored -> {
              fetchStarted.countDown();
              try {
                releaseFetch.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DatabricksSQLException(
                    "Prefetch interrupted", e, DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
              }
              return page(2, 1, 1, null);
            });
    return client;
  }

  private static IDatabricksSession session(IDatabricksClient client) {
    IDatabricksSession session = mock(IDatabricksSession.class);
    when(session.getDatabricksClient()).thenReturn(client);
    return session;
  }

  private static ResultData page(int value, long chunkIndex, long rowOffset, Long nextChunkIndex)
      throws Exception {
    return new ResultData()
        .setArrowBatches(Collections.singletonList(arrowBatch(value)))
        .setRowCount(1L)
        .setChunkIndex(chunkIndex)
        .setRowOffset(rowOffset)
        .setNextChunkIndex(nextChunkIndex);
  }

  private static ResultManifest manifest() {
    return new ResultManifest()
        .setFormat(Format.ARROW_BATCH)
        .setSchema(
            new ResultSchema()
                .setColumnCount(1L)
                .setColumns(Collections.singletonList(intColumn())))
        .setArrowSchema(arrowSchema());
  }

  private static ColumnInfo intColumn() {
    return new ColumnInfo()
        .setName("value")
        .setPosition(0L)
        .setTypeName(ColumnInfoTypeName.INT)
        .setTypeText("INT");
  }

  private static byte[] arrowSchema() {
    org.apache.arrow.vector.types.pojo.Field field =
        new org.apache.arrow.vector.types.pojo.Field(
            "value", new FieldType(true, new ArrowType.Int(32, true), null), null);
    try {
      return SchemaUtility.serialize(new Schema(Collections.singletonList(field)));
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static ArrowBatch arrowBatch(int value) throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        IntVector vector = new IntVector("value", allocator)) {
      vector.allocateNew(1);
      vector.set(0, value);
      vector.setValueCount(1);
      try (VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
        root.setRowCount(1);
        try (ArrowRecordBatch recordBatch = new VectorUnloader(root).getRecordBatch()) {
          MessageSerializer.serialize(new WriteChannel(Channels.newChannel(output)), recordBatch);
        }
      }
      return new ArrowBatch().setBatch(output.toByteArray()).setRowCount(1L);
    }
  }

  private static Thread worker(ArrowBatchChunkProvider provider) throws Exception {
    return field(provider, "worker", Thread.class);
  }

  private static List<ArrowResultChunk> bufferedChunks(ArrowBatchChunkProvider provider)
      throws Exception {
    ReentrantLock lock = field(provider, "lock", ReentrantLock.class);
    lock.lock();
    try {
      ArrayDeque<?> pending = field(provider, "pendingChunks", ArrayDeque.class);
      List<ArrowResultChunk> chunks = new ArrayList<>();
      for (Object chunk : pending) {
        chunks.add((ArrowResultChunk) chunk);
      }
      return chunks;
    } finally {
      lock.unlock();
    }
  }

  private static <T> T field(Object target, String name, Class<T> type) throws Exception {
    Class<?> current = target.getClass();
    while (current != null) {
      try {
        Field field = current.getDeclaredField(name);
        field.setAccessible(true);
        return type.cast(field.get(target));
      } catch (NoSuchFieldException ignored) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchFieldException(name);
  }

  private static void awaitThreadState(Thread thread, Thread.State expected)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (thread.getState() != expected && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(expected, thread.getState(), "thread did not reach expected state");
  }
}
