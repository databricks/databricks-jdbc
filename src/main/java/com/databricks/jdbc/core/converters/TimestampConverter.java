package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.TimeZone;

public class TimestampConverter extends AbstractObjectConverter {
  /* We are not accounting for local Timezone in this class. We are storing GMT timestamp only (for uniformity) */
  private Timestamp object;

  public TimestampConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      try {
        this.object = Timestamp.valueOf((String) object);
      } catch (Exception e) {
        Instant instant = Instant.parse((String) object);
        this.object = Timestamp.from(instant);
      }
    } else {
      this.object = (Timestamp) object;
    }
  }

  @Override
  public Timestamp convertToTimestamp() throws DatabricksSQLException {
    return this.object;
  }

  @Override
  public Date convertToDate() throws DatabricksSQLException {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    return new Date(this.convertToLong());
  }

  @Override
  public long convertToLong() throws DatabricksSQLException {
    return this.object.toInstant().getEpochSecond() * 1000L; // epoch milliseconds
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return this.object.toInstant().toString();
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    return BigInteger.valueOf(this.convertToLong());
  }
}
