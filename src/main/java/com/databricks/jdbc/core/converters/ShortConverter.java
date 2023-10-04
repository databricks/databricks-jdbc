package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class ShortConverter extends AbstractObjectConverter {

    private short object;
    public ShortConverter(Object object) throws DatabricksSQLException {
        super(object);
        if (object instanceof String) {
            this.object = Short.parseShort((String) object);
        }
        else {
            this.object = (short) object;
        }
    }

    @Override
    public short convertToShort() throws DatabricksSQLException {
        return this.object;
    }

    @Override
    public boolean convertToBoolean() throws DatabricksSQLException {
        return (this.object == 0 ? false : true);
    }

    @Override
    public byte convertToByte() throws DatabricksSQLException {
        if(this.object >= Byte.MIN_VALUE && this.object <= Byte.MAX_VALUE) {
            return (byte) this.object;
        }
        throw new DatabricksSQLException("Invalid conversion");
    }

    @Override
    public int convertToInt() throws DatabricksSQLException {
        return (int) this.object;
    }

    @Override
    public long convertToLong() throws DatabricksSQLException {
        return (long) this.object;
    }

    @Override
    public float convertToFloat() throws DatabricksSQLException {
        return (float) this.object;
    }

    @Override
    public double convertToDouble() throws DatabricksSQLException {
        return (double) this.object;
    }

    @Override
    public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
        return BigDecimal.valueOf((long) this.object);
    }

    @Override
    public byte[] convertToByteArray() throws DatabricksSQLException {
        return ByteBuffer.allocate(2).putShort(this.object).array();
    }

    @Override
    public String convertToString() throws DatabricksSQLException {
        return String.valueOf(this.object);
    }
}
