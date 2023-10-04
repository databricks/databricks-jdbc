package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

public class TimestampConverter extends AbstractObjectConverter {

    private Timestamp object;
    public TimestampConverter(Object object) throws DatabricksSQLException {
        super(object);
        if (object instanceof String) {
            this.object = Timestamp.valueOf((String) object);
        }
        else {
            this.object = (Timestamp) object;
        }
    }

    @Override
    public Timestamp convertToTimestamp() throws DatabricksSQLException {
        return this.object;
    }

    @Override
    public Date convertToDate() throws DatabricksSQLException {
        return Date.valueOf(this.object.toLocalDateTime().toLocalDate());
    }

    @Override
    public long convertToLong() throws DatabricksSQLException {
        return this.object.getTime();
    }

    @Override
    public String convertToString() throws DatabricksSQLException {
        return this.object.toString();
    }
}
