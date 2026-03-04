package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the patched allocator does not put the JVM into GC pressure and cause it to OOM
 * (OutOfMemoryError).
 */
public class DatabricksArrowPatchMemoryUsageTest {
  /** Path to an arrow chunk. */
  private static final Path ARROW_CHUNK_PATH = Path.of("arrow", "chunk_all_types.arrow");

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksArrowPatchMemoryUsageTest.class);

  private interface BufferAllocatorFactory {
    BufferAllocator create();
  }

  /**
   * Repeatedly parse an Arrow stream file with low JVM memory -Xmx100m and verify no OOM occurs.
   */
  @Test
  public void testMemoryUsageOfDatabricksBufferAllocator() throws Exception {
    logger.info("Testing memory usage of DatabricksBufferAllocator");
    testMemoryUsageOfBufferAllocator(DatabricksBufferAllocator::new);
  }

  public void testMemoryUsageOfBufferAllocator(BufferAllocatorFactory factory) throws Exception {
    for (int i = 0; i < 1000; i++) {
      try (BufferAllocator allocator = factory.create()) {
        long recordCount = parseArrowStream(ARROW_CHUNK_PATH, allocator);
        if (i % 100 == 0) {
          logger.info("Iteration {}: Parsed {} records.", i, recordCount);
        }
      }
    }
  }

  /**
   * Parse the Arrow stream file stored at {@code filePath}, access every value, and return the
   * record count.
   */
  private long parseArrowStream(Path filePath, BufferAllocator allocator) throws IOException {
    long recordCount = 0;

    Random random = new Random();
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

        // Access each value without retaining references to avoid heap pressure.
        try {
          for (int recordIndex = 0; recordIndex < root.getRowCount(); recordIndex++) {
            // Add logging side effects to prevent JVM from optimizing out this code path.
            HashMap<String, Object> record = new HashMap<>();
            for (ValueVector valueVector : valueVectors) {
              record.put(valueVector.getField().getName(), valueVector.getObject(recordIndex));
            }
            if (random.nextInt(10_000) < 2) {
              logger.trace("Read record with {} keys", record.size());
            }

            recordCount++;
          }
        } finally {
          // Close all transferred vectors to prevent memory leak
          valueVectors.forEach(ValueVector::close);
        }
      }
    }

    return recordCount;
  }

  /**
   * @return an input stream for the filePath.
   */
  private InputStream getStream(Path filePath) throws IOException {
    InputStream arrowStream =
        this.getClass().getClassLoader().getResourceAsStream(filePath.toString());
    assertNotNull(arrowStream, filePath + " not found");
    return arrowStream;
  }
}
