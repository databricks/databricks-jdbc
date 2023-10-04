package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class ShortConverterTest {

    private short NON_ZERO_OBJECT = 10;
    private short ZERO_OBJECT = 0;

    @Test
    public void testConvertToByte() throws DatabricksSQLException {
        assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToByte(), (byte) 10);
        assertEquals(new ShortConverter(ZERO_OBJECT).convertToByte(), (byte) 0);

        short shortThatDoesNotFitInByte = 257;
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new ShortConverter(shortThatDoesNotFitInByte).convertToByte());
        assertTrue(exception.getMessage().contains("Invalid conversion"));
    }

    @Test
    public void testConvertToShort() throws DatabricksSQLException {
        assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToShort(), (short) 10);
        assertEquals(new ShortConverter(ZERO_OBJECT).convertToShort(), (short) 0);
    }

    @Test
    public void testConvertToInt() throws DatabricksSQLException {
        assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToInt(), (int) 10);
        assertEquals(new ShortConverter(ZERO_OBJECT).convertToInt(), (int) 0);
    }

    @Test
    public void testConvertToLong() throws DatabricksSQLException {
        assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToLong(), 10L);
        assertEquals(new ShortConverter(ZERO_OBJECT).convertToLong(), 0L);
    }

    @Test
    public void testConvertToFloat() throws DatabricksSQLException {
        assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToFloat(), 10f);
        assertEquals(new ShortConverter(ZERO_OBJECT).convertToFloat(), 0f);
    }

    @Test
    public void testConvertToDouble() throws DatabricksSQLException {
        assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToDouble(), (double) 10);
        assertEquals(new ShortConverter(ZERO_OBJECT).convertToDouble(), (double) 0);
    }

    @Test
    public void testConvertToBigDecimal() throws DatabricksSQLException {
        assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(10));
        assertEquals(new ShortConverter(ZERO_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(0));
    }

    @Test
    public void testConvertToBoolean() throws DatabricksSQLException {
        assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToBoolean(), true);
        assertEquals(new ShortConverter(ZERO_OBJECT).convertToBoolean(), false);
    }

    @Test
    public void testConvertToByteArray() throws DatabricksSQLException {
        assertTrue(Arrays.equals(new ShortConverter(NON_ZERO_OBJECT).convertToByteArray(),
                ByteBuffer.allocate(2).putShort((short) 10).array()));
        assertTrue(Arrays.equals(new ShortConverter(ZERO_OBJECT).convertToByteArray(),
                ByteBuffer.allocate(2).putShort((short) 0).array()));
    }

    @Test
    public void testConvertToChar() throws DatabricksSQLException {
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new ShortConverter(NON_ZERO_OBJECT).convertToChar());
        assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
    }

    @Test
    public void testConvertToString() throws DatabricksSQLException {
        assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToString(), "10");
        assertEquals(new ShortConverter(ZERO_OBJECT).convertToString(), "0");
    }

    @Test
    public void testConvertToTimestamp() throws DatabricksSQLException {
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new ShortConverter(NON_ZERO_OBJECT).convertToTimestamp());
        assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
    }

    @Test
    public void testConvertToDate() throws DatabricksSQLException {
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new ShortConverter(NON_ZERO_OBJECT).convertToDate());
        assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
    }
}
