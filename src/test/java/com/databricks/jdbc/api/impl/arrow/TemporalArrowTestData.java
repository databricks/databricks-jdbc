package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.IExecutionResult;
import java.io.ByteArrayOutputStream;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

/**
 * Arrow IPC data with DATE, TIMESTAMP (UTC session), TIMESTAMP_NTZ and INT columns for {@link
 * IExecutionResult#getTemporalObject} tests. Row 0 holds BC and pre-1582 values; row 1 holds nulls.
 */
final class TemporalArrowTestData {

  static final LocalDate DATE = LocalDate.of(1582, 10, 5); // epoch day -141437
  static final OffsetDateTime TIMESTAMP = OffsetDateTime.parse("-2015-01-01T00:00:00Z");
  static final LocalDateTime TIMESTAMP_NTZ = LocalDateTime.of(1000, 1, 1, 0, 0);
  static final int INT = 19598;

  private TemporalArrowTestData() {}

  /** Returns the Arrow IPC bytes, with the schema embedded. */
  static byte[] create() {
    try (BufferAllocator allocator = new RootAllocator();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DateDayVector dateVector = new DateDayVector("date_column", allocator);
        TimeStampMicroTZVector timestampVector =
            new TimeStampMicroTZVector("timestamp_column", allocator, "UTC");
        TimeStampMicroVector timestampNtzVector =
            new TimeStampMicroVector("timestamp_ntz_column", allocator);
        IntVector intVector = new IntVector("int_column", allocator)) {
      dateVector.allocateNew(2);
      timestampVector.allocateNew(2);
      timestampNtzVector.allocateNew(2);
      intVector.allocateNew(2);

      dateVector.set(0, (int) DATE.toEpochDay());
      timestampVector.set(0, -125754422400000000L); // TO_TIMESTAMP('2016 bc', 'yyyy G')
      timestampNtzVector.set(0, -30610224000000000L); // TIMESTAMP_NTZ '1000-01-01 00:00:00'
      intVector.set(0, INT);
      dateVector.setNull(1);
      timestampVector.setNull(1);
      timestampNtzVector.setNull(1);
      intVector.setNull(1);

      try (VectorSchemaRoot root =
              VectorSchemaRoot.of(dateVector, timestampVector, timestampNtzVector, intVector);
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
        root.setRowCount(2);
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      return out.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test Arrow data", e);
    }
  }

  /**
   * Reads both rows of {@link #create()} and checks that {@code getTemporalObject} returns the
   * exact values, falls back to {@code getObject} for the INT column, and leaves {@code getObject}
   * on the legacy {@code java.sql} values.
   */
  static void assertRows(IExecutionResult result) throws SQLException {
    assertTrue(result.next());
    assertEquals(DATE, result.getTemporalObject(0));
    assertEquals(TIMESTAMP, result.getTemporalObject(1));
    assertEquals(TIMESTAMP_NTZ, result.getTemporalObject(2));
    assertEquals(INT, result.getTemporalObject(3));
    assertEquals(Date.valueOf(DATE), result.getObject(0));
    assertInstanceOf(Timestamp.class, result.getObject(1));

    assertTrue(result.next());
    for (int column = 0; column < 4; column++) {
      assertNull(result.getTemporalObject(column));
    }
    assertFalse(result.next());
  }
}
