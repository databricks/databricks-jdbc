package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test the patched allocator works. */
public class DatabricksArrowPatchTest {
  private static final Logger logger = LoggerFactory.getLogger(DatabricksArrowPatchTest.class);

  /** Path to an arrow chunk. */
  private static final Path ARROW_CHUNK_PATH = Path.of("arrow", "chunk_all_types.arrow");

  /** Path to a LZ4 compressed arrow chunk. */
  private static final Path ARROW_CHUNK_COMPRESSED_PATH =
      Path.of("arrow", "chunk_all_types.arrow.lz4");

  /** Compressed Arrow file suffix. */
  private static final String ARROW_CHUNK_COMPRESSED_FILE_SUFFIX = ".lz4";

  /** Default number of concurrent threads. */
  private static final int DEFAULT_NUM_THREADS = Runtime.getRuntime().availableProcessors();

  /** System property name to set the number of threads */
  private static final String NUM_THREADS_PROPERTY_NAME = "test.arrow.num.threads";

  /** Default iterations per thread. */
  private static final int DEFAULT_CONCURRENT_ITERATIONS_PER_THREAD = 100;

  /** System property name to set the number of iterations per thread. */
  private static final String CONCURRENT_ITERATIONS_PER_THREAD_PROPERTY_NAME =
      "test.arrow.iterations.per.thread";

  /**
   * Test exception is thrown when jvm arg "--add-opens=java.base/java.nio=ALL-UNNAMED" is missing
   * on JVM >= 17.
   */
  @Test
  @Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
  @EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
  public void testArrowThrowsExceptionOnMissingAddOpensJvmArgs() {
    Throwable throwable = null;
    try {
      RootAllocator allocator = new RootAllocator();
      ArrowBuf buffer = allocator.buffer(64);
      buffer.writeByte(0);
      allocator.close(); // Unreachable code.
    } catch (Throwable t) {
      throwable = t;
    }

    assertNotNull(throwable);
    for (var cause = throwable; cause != null; cause = cause.getCause()) {
      logger.info("Throwable in chain: {} - {}", cause.getClass().getName(), cause.getMessage());
    }
  }

  /**
   * Test patched Arrow buffer allocator works when jvm arg
   * "--add-opens=java.base/java.nio=ALL-UNNAMED" is missing.
   */
  @Test
  @Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
  @EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
  public void testPatchedArrowWorksWithMissingAddOpensJvmArgs() throws IOException {
    for (Path path : Arrays.asList(ARROW_CHUNK_PATH, ARROW_CHUNK_COMPRESSED_PATH)) {
      try (DatabricksBufferAllocator allocator = new DatabricksBufferAllocator()) {
        List<Map<String, Object>> records = parseArrowStream(path, allocator);
        assertFalse(records.isEmpty(), "Some records should be parsed");

        logger.info("Parsed {} records from path {}", records.size(), path);
      }
    }
  }

  /** Parse files concurrently. Test for any memory leaks */
  @Test
  public void testConcurrentExecution() {
    int numThreads =
        System.getProperty(NUM_THREADS_PROPERTY_NAME) == null
            ? DEFAULT_NUM_THREADS
            : Integer.parseInt(System.getProperty(NUM_THREADS_PROPERTY_NAME));
    int iterationsPerThread =
        System.getProperty(CONCURRENT_ITERATIONS_PER_THREAD_PROPERTY_NAME) == null
            ? DEFAULT_CONCURRENT_ITERATIONS_PER_THREAD
            : Integer.parseInt(System.getProperty(CONCURRENT_ITERATIONS_PER_THREAD_PROPERTY_NAME));

    int totalIterations = numThreads * iterationsPerThread;
    logger.info("Num threads {}, Total iterations: {}", numThreads, totalIterations);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      IntStream.range(0, totalIterations)
          .mapToObj(
              i ->
                  executor.submit(
                      () -> {
                        try {
                          parseArrowStream(
                              i % 2 == 0 ? ARROW_CHUNK_PATH : ARROW_CHUNK_COMPRESSED_PATH,
                              new DatabricksBufferAllocator());
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }))
          .forEach(
              future -> {
                try {
                  future.get();
                } catch (InterruptedException | ExecutionException e) {
                  throw new RuntimeException(e);
                }
              });
    } finally {
      executor.shutdownNow();
    }
  }

  /** Test that the patched DatabricksArrowBuf parses records correctly. */
  @Test
  public void testPatchedArrowParsing() throws IOException {
    testParsing(ARROW_CHUNK_PATH);
    testParsing(ARROW_CHUNK_COMPRESSED_PATH);
  }

  /**
   * Parse the Arrow stream file at {@code filePath} and compare the records returned by native
   * Arrow and patched Arrow.
   */
  private void testParsing(Path filePath) throws IOException {
    try (RootAllocator rootAllocator = new RootAllocator();
        DatabricksBufferAllocator patchedAllocator = new DatabricksBufferAllocator()) {

      // Parse with Arrow.
      logger.info("Parsing {} with Arrow RootAllocator", filePath);
      List<Map<String, Object>> rootAllocatorRecords = parseArrowStream(filePath, rootAllocator);
      logger.info("RootAllocator records: {}", rootAllocatorRecords.size());

      // Parse with Patched Arrow.
      logger.info("Parsing {} with Arrow patched DatabricksBufferAllocator", filePath);
      List<Map<String, Object>> patchedAllocatorRecords =
          parseArrowStream(filePath, patchedAllocator);
      logger.info("DatabricksBufferAllocator records: {}", patchedAllocatorRecords.size());

      // Assert that records exist and same number of records are parsed.
      assertFalse(rootAllocatorRecords.isEmpty(), "Some records should be parsed");
      assertEquals(
          rootAllocatorRecords.size(),
          patchedAllocatorRecords.size(),
          "Both should parse same number of records");

      // Log a sample record.
      logger.info("Sample record {}", rootAllocatorRecords.get(0));

      // Compare all records parsed using both allocators.
      for (int i = 0; i < patchedAllocatorRecords.size(); i++) {
        Map<String, Object> patchedRecord = patchedAllocatorRecords.get(i);
        Map<String, Object> rootRecord = rootAllocatorRecords.get(i);
        assertEquals(
            rootRecord.keySet(), patchedRecord.keySet(), "Same number of columns should be parsed");

        for (String key : patchedRecord.keySet()) {
          Object patchedColumn = patchedRecord.get(key);
          Object rootColumn = rootRecord.get(key);
          assertTrue(
              deepEquals(rootColumn, patchedColumn),
              "Column " + key + " should be same at row " + i);
        }
      }
    }
  }

  /**
   * Deep equality check that handles byte[] (which uses reference equality by default) and
   * recursively checks Lists and Maps that may contain byte[] values (e.g., struct or map columns
   * with binary fields).
   */
  @SuppressWarnings("unchecked")
  private boolean deepEquals(Object a, Object b) {
    if (a == b) return true;
    if (a == null || b == null) return false;
    if (a instanceof byte[] && b instanceof byte[]) {
      return Arrays.equals((byte[]) a, (byte[]) b);
    }
    if (a instanceof List && b instanceof List) {
      List<Object> listA = (List<Object>) a;
      List<Object> listB = (List<Object>) b;
      if (listA.size() != listB.size()) return false;
      for (int i = 0; i < listA.size(); i++) {
        if (!deepEquals(listA.get(i), listB.get(i))) return false;
      }
      return true;
    }
    if (a instanceof Map && b instanceof Map) {
      Map<Object, Object> mapA = (Map<Object, Object>) a;
      Map<Object, Object> mapB = (Map<Object, Object>) b;
      if (mapA.size() != mapB.size()) return false;
      for (Map.Entry<Object, Object> entry : mapA.entrySet()) {
        // For binary map keys, we need to find a matching key in mapB
        Object matchedValue = null;
        boolean found = false;
        for (Map.Entry<Object, Object> entryB : mapB.entrySet()) {
          if (deepEquals(entry.getKey(), entryB.getKey())) {
            matchedValue = entryB.getValue();
            found = true;
            break;
          }
        }
        if (!found || !deepEquals(entry.getValue(), matchedValue)) return false;
      }
      return true;
    }
    return a.equals(b);
  }

  /** Parse the Arrow stream file stored at {@code filePath} and return the records in the file. */
  private List<Map<String, Object>> parseArrowStream(Path filePath, BufferAllocator allocator)
      throws IOException {
    ArrayList<Map<String, Object>> records = new ArrayList<>();

    try (InputStream arrowStream = getStream(filePath);
        ArrowStreamReader reader = new ArrowStreamReader(arrowStream, allocator)) {
      // Iterate over batches.
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();

        // Transfer all vectors.
        List<ValueVector> valueVectors =
            root.getFieldVectors().stream()
                .map(
                    fieldVector -> {
                      TransferPair transferPair = fieldVector.getTransferPair(allocator);
                      transferPair.transfer();
                      return transferPair.getTo();
                    })
                .collect(Collectors.toList());

        // Parse and populate each record/row in this batch.
        try {
          for (int recordIndex = 0; recordIndex < root.getRowCount(); recordIndex++) {
            HashMap<String, Object> record = new HashMap<>();
            for (ValueVector valueVector : valueVectors) {
              record.put(valueVector.getField().getName(), valueVector.getObject(recordIndex));
            }
            records.add(record);
          }
        } finally {
          // Close all transferred vectors to prevent memory leak
          valueVectors.forEach(ValueVector::close);
        }
      }
    }

    return records;
  }

  /**
   * @return an input stream for the filePath.
   */
  private InputStream getStream(Path filePath) throws IOException {
    InputStream arrowStream = getClass().getClassLoader().getResourceAsStream(filePath.toString());
    assertNotNull(arrowStream, filePath + " not found");
    return filePath.toString().endsWith(ARROW_CHUNK_COMPRESSED_FILE_SUFFIX)
        ? new LZ4FrameInputStream(arrowStream)
        : arrowStream;
  }
}
