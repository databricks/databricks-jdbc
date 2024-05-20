package com.databricks.jdbc.integration.fakeservice;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;

/** Utility class for JSON operations. */
public class JsonUtils {

  /** {@link ObjectMapper} instance holder. */
  private static final InheritableThreadLocal<ObjectMapper> objectMapperHolder =
      new InheritableThreadLocal<>() {
        @Override
        protected ObjectMapper initialValue() {
          ObjectMapper objectMapper =
              new ObjectMapper(
                  new JsonFactoryBuilder()
                      .streamReadConstraints(
                          StreamReadConstraints.builder()
                              .maxStringLength(Math.toIntExact(30000000))
                              .build())
                      .build());
          objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
          objectMapper.configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);
          objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
          objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
          objectMapper.configure(JsonParser.Feature.IGNORE_UNDEFINED, true);
          objectMapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
          objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
          objectMapper.registerModule(new JavaTimeModule());
          objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
          objectMapper.enable(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION);
          return objectMapper;
        }
      };

  /** Read the given JSON string and return the object of the given class. */
  public static <T> T read(String json, Class<T> clazz) throws JsonProcessingException {
    return objectMapperHolder.get().readValue(json, clazz);
  }

  /** Read the given byte array and return the object of the given class. */
  public static <T> T read(byte[] stream, Class<T> clazz) throws IOException {
    return objectMapperHolder.get().readValue(stream, clazz);
  }

  /** Write the given object to a JSON string. */
  public static <T> String write(T object, Class<?> view) throws JsonProcessingException {
    ObjectWriter objectWriter = objectMapperHolder.get().writerWithDefaultPrettyPrinter();
    if (view != null) {
      objectWriter = objectWriter.withView(view);
    }
    return objectWriter.writeValueAsString(object);
  }
}
