package com.databricks.jdbc.dbclient.impl.thrift;

import static org.junit.jupiter.api.Assertions.fail;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.thrift.TFieldIdEnum;
import org.junit.jupiter.api.Test;

/**
 * Validates that all Thrift-generated classes comply with field ID constraints.
 *
 * <p>Field IDs in Thrift must stay below 3329 to avoid conflicts with reserved ranges and ensure
 * compatibility with various Thrift implementations and protocols.
 */
public class ThriftGeneratedFieldIdTest {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(ThriftGeneratedFieldIdTest.class);

  private static final int MAX_ALLOWED_FIELD_ID = 3329;
  private static final String GENERATED_PACKAGE =
      "com.databricks.jdbc.model.client.thrift.generated";
  private static final String GENERATED_DIR =
      "src/main/java/com/databricks/jdbc/model/client/thrift/generated";

  @Test
  public void testAllThriftFieldIdsAreWithinAllowedRange() throws IOException {
    List<String> violations = new ArrayList<>();

    Path generatedDirPath = Paths.get(GENERATED_DIR);

    if (!Files.exists(generatedDirPath)) {
      fail("Generated directory does not exist: " + GENERATED_DIR);
    }

    // Find all Java files that might contain Thrift-generated classes
    try (Stream<Path> javaFiles =
        Files.walk(generatedDirPath).filter(path -> path.toString().endsWith(".java"))) {
      javaFiles.forEach(javaFile -> checkFileForFieldIdViolations(javaFile, violations));
    }

    if (!violations.isEmpty()) {
      StringBuilder errorMessage = new StringBuilder();
      errorMessage
          .append("Found Thrift field IDs that exceed the maximum allowed value of ")
          .append(MAX_ALLOWED_FIELD_ID)
          .append(".\nThis can cause compatibility issues and conflicts with reserved ID ranges.\n")
          .append("Violations found:\n");

      for (String violation : violations) {
        errorMessage.append("  - ").append(violation).append("\n");
      }

      fail(errorMessage.toString());
    }
  }

  /**
   * Examines a Java file to determine if it contains a Thrift-generated class and validates that
   * all field IDs are within the allowed range.
   */
  private void checkFileForFieldIdViolations(Path javaFile, List<String> violations) {
    String fileName = javaFile.getFileName().toString();

    if (!fileName.endsWith(".java")) {
      return;
    }

    String className = fileName.substring(0, fileName.length() - 5);
    String fullClassName = GENERATED_PACKAGE + "." + className;

    try {
      Class<?> clazz = Class.forName(fullClassName);

      // Look for the _Fields enum that Thrift generates for struct classes
      // This enum contains metadata about each field including its ID
      for (Class<?> innerClass : clazz.getDeclaredClasses()) {
        if (isThriftFieldsEnum(innerClass)) {
          validateAllFieldIdsInClass(innerClass, className, violations);
          break;
        }
      }
    } catch (ClassNotFoundException e) {
      // File exists but class can't be loaded - might not be compiled
      // or might not be a valid Thrift-generated class
      LOGGER.error(e, "Unable to find class " + fullClassName);
    }
  }

  /** Checks if the given class is a Thrift-generated _Fields enum that contains field metadata. */
  private boolean isThriftFieldsEnum(Class<?> innerClass) {
    return "_Fields".equals(innerClass.getSimpleName())
        && innerClass.isEnum()
        && TFieldIdEnum.class.isAssignableFrom(innerClass);
  }

  /**
   * Validates that all field IDs in a Thrift class are within the allowed range. Reports any fields
   * that have IDs >= MAX_ALLOWED_FIELD_ID.
   */
  private void validateAllFieldIdsInClass(
      Class<?> fieldsEnum, String className, List<String> violations) {
    Object[] fieldDefinitions = fieldsEnum.getEnumConstants();

    if (fieldDefinitions != null) {
      for (Object fieldDefinition : fieldDefinitions) {
        TFieldIdEnum thriftField = (TFieldIdEnum) fieldDefinition;
        short fieldId = thriftField.getThriftFieldId();

        if (fieldId >= MAX_ALLOWED_FIELD_ID) {
          String fieldName = fieldDefinition.toString();
          violations.add(
              String.format(
                  "%s._Fields.%s has field ID %d (exceeds maximum of %d)",
                  className, fieldName, fieldId, MAX_ALLOWED_FIELD_ID - 1));
        }
      }
    }
  }
}
