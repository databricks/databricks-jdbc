package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Arrays;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Test temporal Arrow data types (dates, timestamps, time, duration, intervals). */
@Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
@EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
public class DatabricksArrowPatchTemporalTypesTest extends AbstractDatabricksArrowPatchTypesTest {

  /** Test read and write of temporal types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testTemporalTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testTemporal = new TestTemporalTypes();
    byte[] data = writeData(testTemporal, totalRows, writeAllocator);
    readAndValidate(testTemporal, data, readAllocator);
  }

  /** Test temporal types */
  private class TestTemporalTypes implements DataTester {
    private final Field dateDayField;
    private final Field timestampField;
    private final Field timestampMicroField;
    private final Field timeSecField;
    private final Field timeNanoField;
    private final Field durationMicrosecondField;
    private final Field intervalYearField;
    private final Field intervalDayField;
    private final Field intervalMonthDayNanoField;
    private final Schema schema;

    TestTemporalTypes() {
      dateDayField = newDateDayField();
      timestampField = newTimestampMilliField();
      timestampMicroField = newTimestampMicroField();
      timeSecField = newTimeSecField();
      timeNanoField = newTimeNanoField();
      durationMicrosecondField = newDurationMicrosecondField();
      intervalYearField = newIntervalYearField();
      intervalDayField = newIntervalDayField();
      intervalMonthDayNanoField = newIntervalMonthDayNanoField();
      schema =
          new Schema(
              Arrays.asList(
                  dateDayField,
                  timestampField,
                  timestampMicroField,
                  timeSecField,
                  timeNanoField,
                  durationMicrosecondField,
                  intervalYearField,
                  intervalDayField,
                  intervalMonthDayNanoField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      DateDayVector dateDayVector =
          (DateDayVector) vectorSchemaRoot.getVector(dateDayField.getName());
      TimeStampMilliTZVector timestampVector =
          (TimeStampMilliTZVector) vectorSchemaRoot.getVector(timestampField.getName());
      TimeStampMicroTZVector timestampMicroVector =
          (TimeStampMicroTZVector) vectorSchemaRoot.getVector(timestampMicroField.getName());
      TimeSecVector timeSecVector =
          (TimeSecVector) vectorSchemaRoot.getVector(timeSecField.getName());
      TimeNanoVector timeNanoVector =
          (TimeNanoVector) vectorSchemaRoot.getVector(timeNanoField.getName());
      DurationVector durationVector =
          (DurationVector) vectorSchemaRoot.getVector(durationMicrosecondField.getName());
      IntervalYearVector intervalYearVector =
          (IntervalYearVector) vectorSchemaRoot.getVector(intervalYearField.getName());
      IntervalDayVector intervalDayVector =
          (IntervalDayVector) vectorSchemaRoot.getVector(intervalDayField.getName());
      IntervalMonthDayNanoVector intervalMonthDayNanoVector =
          (IntervalMonthDayNanoVector)
              vectorSchemaRoot.getVector(intervalMonthDayNanoField.getName());

      // Set dates (days since epoch).
      dateDayVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          dateDayVector.setNull(i);
        } else {
          dateDayVector.set(i, getDateDay(i));
        }
      }

      // Set timestamps (milliseconds since epoch).
      timestampVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          timestampVector.setNull(i);
        } else {
          timestampVector.set(i, getTimestampMilli(i));
        }
      }

      // Set timestamps (microseconds since epoch).
      timestampMicroVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          timestampMicroVector.setNull(i);
        } else {
          timestampMicroVector.set(i, getTimestampMicro(i));
        }
      }

      // Set times (seconds since midnight).
      timeSecVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          timeSecVector.setNull(i);
        } else {
          timeSecVector.set(i, getTimeSec(i));
        }
      }

      // Set times (nanoseconds since midnight).
      timeNanoVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          timeNanoVector.setNull(i);
        } else {
          timeNanoVector.set(i, getTimeNano(i));
        }
      }

      // Set durations (microseconds).
      durationVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          durationVector.setNull(i);
        } else {
          durationVector.set(i, getDurationMicroseconds(i));
        }
      }

      // Set interval year-months.
      intervalYearVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          intervalYearVector.setNull(i);
        } else {
          intervalYearVector.set(i, getIntervalYearMonth(i));
        }
      }

      // Set interval day-times.
      intervalDayVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          intervalDayVector.setNull(i);
        } else {
          intervalDayVector.set(i, getIntervalDayDays(i), getIntervalDayMillis(i));
        }
      }

      // Set interval month-day-nanos.
      intervalMonthDayNanoVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          intervalMonthDayNanoVector.setNull(i);
        } else {
          intervalMonthDayNanoVector.set(
              i,
              getIntervalMonthDayNanoMonths(i),
              getIntervalMonthDayNanoDays(i),
              getIntervalMonthDayNanoNanos(i));
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      DateDayVector dateDayVector =
          (DateDayVector) vectorSchemaRoot.getVector(dateDayField.getName());
      TimeStampMilliTZVector timestampVector =
          (TimeStampMilliTZVector) vectorSchemaRoot.getVector(timestampField.getName());
      TimeStampMicroTZVector timestampMicroVector =
          (TimeStampMicroTZVector) vectorSchemaRoot.getVector(timestampMicroField.getName());
      TimeSecVector timeSecVector =
          (TimeSecVector) vectorSchemaRoot.getVector(timeSecField.getName());
      TimeNanoVector timeNanoVector =
          (TimeNanoVector) vectorSchemaRoot.getVector(timeNanoField.getName());
      DurationVector durationVector =
          (DurationVector) vectorSchemaRoot.getVector(durationMicrosecondField.getName());
      IntervalYearVector intervalYearVector =
          (IntervalYearVector) vectorSchemaRoot.getVector(intervalYearField.getName());
      IntervalDayVector intervalDayVector =
          (IntervalDayVector) vectorSchemaRoot.getVector(intervalDayField.getName());
      IntervalMonthDayNanoVector intervalMonthDayNanoVector =
          (IntervalMonthDayNanoVector)
              vectorSchemaRoot.getVector(intervalMonthDayNanoField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate date (days since epoch)
        if (i % 2 == 0) {
          assertTrue(dateDayVector.isNull(i), "Date should be null at index " + i);
        } else {
          assertEquals(getDateDay(i), dateDayVector.get(i), "Date mismatch at index " + i);
        }

        // Validate timestamp (milliseconds since epoch)
        if (i % 2 == 0) {
          assertTrue(timestampVector.isNull(i), "Timestamp should be null at index " + i);
        } else {
          assertEquals(
              getTimestampMilli(i), timestampVector.get(i), "Timestamp mismatch at index " + i);
        }

        // Validate timestamp (microseconds since epoch)
        if (i % 2 == 0) {
          assertTrue(
              timestampMicroVector.isNull(i), "Timestamp micro should be null at index " + i);
        } else {
          assertEquals(
              getTimestampMicro(i),
              timestampMicroVector.get(i),
              "Timestamp micro mismatch at index " + i);
        }

        // Validate time (seconds since midnight)
        if (i % 2 == 0) {
          assertTrue(timeSecVector.isNull(i), "Time should be null at index " + i);
        } else {
          assertEquals(getTimeSec(i), timeSecVector.get(i), "Time mismatch at index " + i);
        }

        // Validate time (nanoseconds since midnight)
        if (i % 2 == 0) {
          assertTrue(timeNanoVector.isNull(i), "Time nano should be null at index " + i);
        } else {
          assertEquals(getTimeNano(i), timeNanoVector.get(i), "Time nano mismatch at index " + i);
        }

        // Validate duration (seconds)
        if (i % 2 == 0) {
          assertTrue(durationVector.isNull(i), "Duration should be null at index " + i);
        } else {
          Duration durationValue = durationVector.getObject(i);
          assertNotNull(durationValue, "Duration should not be null at index " + i);
          assertEquals(
              getDurationMicroseconds(i),
              durationValue.getNano() / (1000),
              "Duration mismatch at index " + i);
        }

        // Validate interval year-month
        if (i % 2 == 0) {
          assertTrue(
              intervalYearVector.isNull(i), "Interval year-month should be null at index " + i);
        } else {
          assertEquals(
              getIntervalYearMonth(i),
              intervalYearVector.get(i),
              "Interval year-month mismatch at index " + i);
        }

        // Validate interval day-time
        if (i % 2 == 0) {
          assertTrue(intervalDayVector.isNull(i), "Interval day-time should be null at index " + i);
        } else {
          NullableIntervalDayHolder holder = new NullableIntervalDayHolder();
          intervalDayVector.get(i, holder);
          assertEquals(getIntervalDayDays(i), holder.days, "Interval days mismatch at index " + i);
          assertEquals(
              getIntervalDayMillis(i),
              holder.milliseconds,
              "Interval milliseconds mismatch at index " + i);
        }

        // Validate interval month-day-nano
        if (i % 2 == 0) {
          assertTrue(
              intervalMonthDayNanoVector.isNull(i),
              "Interval month-day-nano should be null at index " + i);
        } else {
          PeriodDuration value = intervalMonthDayNanoVector.getObject(i);
          assertNotNull(value, "Interval month-day-nano should not be null at index " + i);
          assertEquals(
              getIntervalMonthDayNanoMonths(i),
              value.getPeriod().toTotalMonths(),
              "Interval months mismatch at index " + i);
          assertEquals(
              getIntervalMonthDayNanoDays(i),
              value.getPeriod().getDays(),
              "Interval days mismatch at index " + i);
          assertEquals(
              getIntervalMonthDayNanoNanos(i),
              value.getDuration().toNanos(),
              "Interval nanoseconds mismatch at index " + i);
        }
      }
    }

    private int getIntervalMonthDayNanoMonths(int index) {
      return index % 12;
    }

    private int getIntervalMonthDayNanoDays(int index) {
      return index % 30;
    }

    private long getIntervalMonthDayNanoNanos(int index) {
      return ((long) index * 1_000_000_000L) % 86_400_000_000_000L;
    }
  }
}
