package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class BigDecimalConverterTest {

    private BigDecimal NON_ZERO_OBJECT = BigDecimal.valueOf(10.2);
    private BigDecimal ZERO_OBJECT = BigDecimal.valueOf(0);
    @Test
    public void testConvertToByte() throws DatabricksSQLException {
        assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToByte(), (byte) 10);
        assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToByte(), (byte) 0);

        BigDecimal bigDecimalThatDoesNotFitInByte = BigDecimal.valueOf(257.1);
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new BigDecimalConverter(bigDecimalThatDoesNotFitInByte).convertToByte());
        assertTrue(exception.getMessage().contains("Invalid conversion"));
    }

    @Test
    public void testConvertToShort() throws DatabricksSQLException {
        assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToShort(), (short) 10);
        assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToShort(), (short) 0);


        BigDecimal bigDecimalThatDoesNotFitInInt = BigDecimal.valueOf(32768.1);
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new BigDecimalConverter(bigDecimalThatDoesNotFitInInt).convertToShort());
        assertTrue(exception.getMessage().contains("Invalid conversion"));
    }

    @Test
    public void testConvertToInt() throws DatabricksSQLException {
        assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToInt(), (int) 10);
        assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToInt(), (int) 0);

        BigDecimal bigDecimalThatDoesNotFitInInt = BigDecimal.valueOf(2147483648.1);
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new BigDecimalConverter(bigDecimalThatDoesNotFitInInt).convertToInt());
        assertTrue(exception.getMessage().contains("Invalid conversion"));
    }

    @Test
    public void testConvertToLong() throws DatabricksSQLException {
        assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToLong(), 10L);
        assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToLong(), 0L);

        BigDecimal bigDecimalThatDoesNotFitInInt = BigDecimal.valueOf(9223372036854775808.1);
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new BigDecimalConverter(bigDecimalThatDoesNotFitInInt).convertToLong());
        assertTrue(exception.getMessage().contains("Invalid conversion"));
    }

    @Test
    public void testConvertToFloat() throws DatabricksSQLException {
        assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToFloat(), 10.2f);
        assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToFloat(), 0f);
    }

    @Test
    public void testConvertToDouble() throws DatabricksSQLException {
        assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToDouble(), (double) 10.2);
        assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToDouble(), (double) 0);
    }

    @Test
    public void testConvertToBigDecimal() throws DatabricksSQLException {
        assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(10.2));
        assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(0));
    }

    @Test
    public void testConvertToBoolean() throws DatabricksSQLException {
        assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToBoolean(), true);
        assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToBoolean(), false);
    }

    @Test
    public void testConvertToByteArray() throws DatabricksSQLException {
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new BigDecimalConverter(NON_ZERO_OBJECT).convertToChar());
        assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
    }

    @Test
    public void testConvertToChar() throws DatabricksSQLException {
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new BigDecimalConverter(NON_ZERO_OBJECT).convertToChar());
        assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
    }

    @Test
    public void testConvertToString() throws DatabricksSQLException {
        assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToString(), "10.2");
        assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToString(), "0");
    }

    @Test
    public void testConvertToTimestamp() throws DatabricksSQLException {
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new BigDecimalConverter(NON_ZERO_OBJECT).convertToTimestamp());
        assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
    }

    @Test
    public void testConvertToDate() throws DatabricksSQLException {
        DatabricksSQLException exception =
                assertThrows(DatabricksSQLException.class, () -> new BigDecimalConverter(NON_ZERO_OBJECT).convertToDate());
        assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
    }
}
