package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.TransferPair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Fork(value = 1)
@Measurement(iterations = 20, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 20, time = 100, timeUnit = TimeUnit.MILLISECONDS)
public class ArrowParsingBenchmark {
  /** Path to an arrow chunk. */
  private static final Path ARROW_CHUNK_PATH = Path.of("arrow", "chunk_all_types.arrow");

  /** Path to a LZ4 compressed arrow chunk. */
  private static final Path ARROW_CHUNK_COMPRESSED_PATH =
      Path.of("arrow", "chunk_all_types.arrow.lz4");

  /** Compressed Arrow file suffix. */
  private static final String ARROW_CHUNK_COMPRESSED_FILE_SUFFIX = ".lz4";

  public static void main(String[] args) throws RunnerException {
    Options options =
        new OptionsBuilder().include(ArrowParsingBenchmark.class.getSimpleName()).build();
    new Runner(options).run();
  }

  // Pre-loaded file contents
  private byte[] arrowChunkBytes;
  private byte[] arrowChunkCompressedBytes;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    // Load files into memory once before all benchmark iterations
    arrowChunkBytes = loadFileToMemory(ARROW_CHUNK_PATH);
    arrowChunkCompressedBytes = loadFileToMemory(ARROW_CHUNK_COMPRESSED_PATH);
  }

  private byte[] loadFileToMemory(Path filePath) throws IOException {
    try (InputStream stream =
        getClass().getClassLoader().getResourceAsStream(filePath.toString())) {
      assertNotNull(stream, filePath + " not found");
      return stream.readAllBytes();
    }
  }

  @Benchmark
  public List<Map<String, Object>> parseArrowChunk() throws IOException {
    try (BufferAllocator allocator = new RootAllocator()) {
      return parseArrowStream(arrowChunkBytes, false, allocator);
    }
  }

  @Benchmark
  public List<Map<String, Object>> parseArrowCompressedChunk() throws IOException {
    try (BufferAllocator allocator = new RootAllocator()) {
      return parseArrowStream(arrowChunkCompressedBytes, true, allocator);
    }
  }

  @Benchmark
  public List<Map<String, Object>> parsePatchedArrowChunk() throws IOException {
    try (BufferAllocator allocator = new DatabricksBufferAllocator()) {
      return parseArrowStream(arrowChunkBytes, false, allocator);
    }
  }

  @Benchmark
  public List<Map<String, Object>> parsePatchedArrowCompressedChunk() throws IOException {
    try (BufferAllocator allocator = new DatabricksBufferAllocator()) {
      return parseArrowStream(arrowChunkCompressedBytes, true, allocator);
    }
  }

  /** Parse the Arrow stream file stored at {@code filePath} and return the records in the file. */
  private List<Map<String, Object>> parseArrowStream(
      byte[] arrowChunkBytes, boolean isCompressed, BufferAllocator allocator) throws IOException {
    ArrayList<Map<String, Object>> records = new ArrayList<>();

    InputStream arrowStream = new ByteArrayInputStream(arrowChunkBytes);
    if (isCompressed) {
      arrowStream = new LZ4FrameInputStream(arrowStream);
    }

    try (ArrowStreamReader reader = new ArrowStreamReader(arrowStream, allocator)) {
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
