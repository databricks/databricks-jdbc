# Statement-Level Query Tags — Implementation Spec

## Overview

Add statement-level query tags to the Databricks JDBC driver so users can annotate
individual statement executions for tracking and cost attribution. The server-side
SQL Exec API already supports a `query_tags` field on `ExecuteStatementRequest`
(field 17, `repeated QueryTag`, at `PUBLIC_PREVIEW` stage). The Thrift path supports
per-statement config overrides via `confOverlay` on `TExecuteStatementReq`.

This spec covers both transport paths (SEA and Thrift), validation, public API,
internal wiring, tests, and changelog.

---

## Constraints (from server proto / docs)

| Constraint                    | Value                         |
|-------------------------------|-------------------------------|
| Max tags per query            | 20                            |
| Max key length                | 128 characters                |
| Max value length              | 128 characters                |
| Invalid key characters        | `,` `:` `-` `/` `=` `.`      |
| Values                        | May be null (key-only tags)   |

---

## Files to Create

### 1. `src/main/java/com/databricks/jdbc/model/client/sqlexec/QueryTag.java`

A simple POJO for JSON serialization matching the SEA API `QueryTag` schema.
Follow the existing POJO patterns in the same package (see `PositionalStatementParameterListItem.java`).

```java
package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** Query tag POJO for the SQL Exec API execute-statement request. */
public class QueryTag {

  @JsonProperty("key")
  private String key;

  @JsonProperty("value")
  private String value;

  public QueryTag() {}

  public QueryTag(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public QueryTag setKey(String key) {
    this.key = key;
    return this;
  }

  public String getValue() {
    return value;
  }

  public QueryTag setValue(String value) {
    this.value = value;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    QueryTag that = (QueryTag) o;
    return Objects.equals(key, that.key) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }

  @Override
  public String toString() {
    return new ToStringer(QueryTag.class).add("key", key).add("value", value).toString();
  }
}
```

### 2. `src/test/java/com/databricks/jdbc/model/client/sqlexec/QueryTagTest.java`

```java
package com.databricks.jdbc.model.client.sqlexec;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class QueryTagTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testConstructionWithKeyAndValue() {
    QueryTag tag = new QueryTag("team", "marketing");
    assertEquals("team", tag.getKey());
    assertEquals("marketing", tag.getValue());
  }

  @Test
  public void testConstructionWithNullValue() {
    QueryTag tag = new QueryTag("debug", null);
    assertEquals("debug", tag.getKey());
    assertNull(tag.getValue());
  }

  @Test
  public void testJsonSerializationRoundTrip() throws Exception {
    QueryTag original = new QueryTag("team", "marketing");
    String json = mapper.writeValueAsString(original);
    QueryTag deserialized = mapper.readValue(json, QueryTag.class);
    assertEquals(original, deserialized);
  }

  @Test
  public void testJsonSerializationWithNullValue() throws Exception {
    QueryTag original = new QueryTag("debug", null);
    String json = mapper.writeValueAsString(original);
    QueryTag deserialized = mapper.readValue(json, QueryTag.class);
    assertEquals(original, deserialized);
  }

  @Test
  public void testEqualsAndHashCode() {
    QueryTag a = new QueryTag("k", "v");
    QueryTag b = new QueryTag("k", "v");
    QueryTag c = new QueryTag("k", "other");
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
  }

  @Test
  public void testFluentSetters() {
    QueryTag tag = new QueryTag().setKey("k").setValue("v");
    assertEquals("k", tag.getKey());
    assertEquals("v", tag.getValue());
  }
}
```

### 3. `src/test/java/com/databricks/jdbc/model/client/sqlexec/ExecuteStatementRequestTest.java`

```java
package com.databricks.jdbc.model.client.sqlexec;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ExecuteStatementRequestTest {

  private final ObjectMapper mapper =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  @Test
  public void testSerializationWithQueryTags() throws Exception {
    List<QueryTag> tags =
        Arrays.asList(new QueryTag("team", "marketing"), new QueryTag("dashboard", "abc123"));
    ExecuteStatementRequest request =
        new ExecuteStatementRequest().setStatement("SELECT 1").setQueryTags(tags);

    String json = mapper.writeValueAsString(request);
    ExecuteStatementRequest deserialized = mapper.readValue(json, ExecuteStatementRequest.class);
    assertEquals(request, deserialized);
    assertEquals(2, deserialized.getQueryTags().size());
    assertEquals(new QueryTag("team", "marketing"), deserialized.getQueryTags().get(0));
    assertEquals(new QueryTag("dashboard", "abc123"), deserialized.getQueryTags().get(1));
  }

  @Test
  public void testSerializationWithoutQueryTags() throws Exception {
    ExecuteStatementRequest request = new ExecuteStatementRequest().setStatement("SELECT 1");

    String json = mapper.writeValueAsString(request);
    ExecuteStatementRequest deserialized = mapper.readValue(json, ExecuteStatementRequest.class);
    assertEquals(request, deserialized);
    assertNull(deserialized.getQueryTags());
  }

  @Test
  public void testQueryTagWithNullValueSerialized() throws Exception {
    List<QueryTag> tags = Arrays.asList(new QueryTag("debug", null));
    ExecuteStatementRequest request =
        new ExecuteStatementRequest().setStatement("SELECT 1").setQueryTags(tags);

    String json = mapper.writeValueAsString(request);
    ExecuteStatementRequest deserialized = mapper.readValue(json, ExecuteStatementRequest.class);
    assertEquals(1, deserialized.getQueryTags().size());
    assertEquals("debug", deserialized.getQueryTags().get(0).getKey());
    assertNull(deserialized.getQueryTags().get(0).getValue());
  }

  @Test
  public void testEqualsAndHashCodeWithQueryTags() {
    List<QueryTag> tags = Arrays.asList(new QueryTag("k", "v"));
    ExecuteStatementRequest a = new ExecuteStatementRequest().setStatement("SELECT 1").setQueryTags(tags);
    ExecuteStatementRequest b = new ExecuteStatementRequest().setStatement("SELECT 1").setQueryTags(tags);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }
}
```

---

## Files to Modify

### 4. `src/main/java/com/databricks/jdbc/api/IDatabricksStatement.java`

Add two methods to the public interface. Place them after the existing `getExecutionResult()` method.

```java
  /**
   * Sets query tags for subsequent statement executions. Tags are key-value pairs used for tracking
   * and cost attribution. Values may be null for key-only tags.
   *
   * <p>Passing {@code null} or an empty map clears any previously set tags.
   *
   * @param queryTags a map of tag keys to values, or null to clear
   * @throws SQLException if the statement is closed or tags fail validation
   */
  void setQueryTags(Map<String, String> queryTags) throws SQLException;

  /**
   * Returns the query tags currently set on this statement.
   *
   * @return an unmodifiable map of tag keys to values, or null if no tags are set
   * @throws SQLException if the statement is closed
   */
  Map<String, String> getQueryTags() throws SQLException;
```

**Required import addition:**
```java
import java.util.Map;
```

### 5. `src/main/java/com/databricks/jdbc/api/internal/IDatabricksStatementInternal.java`

Add a getter method for the internal interface (follows the `getMaxRows()` pattern at line 7).
Place it after `getMaxRows()`.

```java
  Map<String, String> getQueryTags() throws DatabricksSQLException;
```

**Required import addition:**
```java
import java.util.Map;
```

### 6. `src/main/java/com/databricks/jdbc/api/impl/DatabricksStatement.java`

#### 6a. Add validation constants (near the top of the class, after existing constants)

```java
  private static final int MAX_QUERY_TAGS = 20;
  private static final int MAX_QUERY_TAG_KEY_LENGTH = 128;
  private static final int MAX_QUERY_TAG_VALUE_LENGTH = 128;
  private static final java.util.regex.Pattern INVALID_KEY_CHARS_PATTERN =
      java.util.regex.Pattern.compile("[,:\\-/=.]");
```

#### 6b. Add field (near other statement fields like `maxRows`, `maxFieldSize`)

```java
  private Map<String, String> queryTags = null;
```

#### 6c. Implement `setQueryTags()` (public API method — throws `SQLException`)

```java
  @Override
  public void setQueryTags(Map<String, String> queryTags) throws SQLException {
    checkIfClosed();
    if (queryTags == null || queryTags.isEmpty()) {
      this.queryTags = null;
      return;
    }
    validateQueryTags(queryTags);
    this.queryTags = Collections.unmodifiableMap(new HashMap<>(queryTags));
  }
```

#### 6d. Implement `getQueryTags()` (satisfies both `IDatabricksStatement` and `IDatabricksStatementInternal`)

```java
  @Override
  public Map<String, String> getQueryTags() throws SQLException {
    checkIfClosed();
    return queryTags;
  }
```

Note: This single method satisfies both interfaces because `DatabricksSQLException extends SQLException`.
The public interface declares `throws SQLException` and the internal interface declares
`throws DatabricksSQLException` — both are satisfied by the implementation throwing `SQLException`.

#### 6e. Add private validation method

```java
  private void validateQueryTags(Map<String, String> tags) throws DatabricksValidationException {
    if (tags.size() > MAX_QUERY_TAGS) {
      throw new DatabricksValidationException(
          format(
              "Too many query tags: %d exceeds maximum of %d", tags.size(), MAX_QUERY_TAGS));
    }
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || key.isEmpty()) {
        throw new DatabricksValidationException("Query tag key must not be null or empty");
      }
      if (key.length() > MAX_QUERY_TAG_KEY_LENGTH) {
        throw new DatabricksValidationException(
            format(
                "Query tag key '%s' exceeds maximum length of %d",
                key, MAX_QUERY_TAG_KEY_LENGTH));
      }
      if (INVALID_KEY_CHARS_PATTERN.matcher(key).find()) {
        throw new DatabricksValidationException(
            format(
                "Query tag key '%s' contains invalid characters. Keys must not contain: , : - / = .",
                key));
      }
      if (value != null && value.length() > MAX_QUERY_TAG_VALUE_LENGTH) {
        throw new DatabricksValidationException(
            format(
                "Query tag value for key '%s' exceeds maximum length of %d",
                key, MAX_QUERY_TAG_VALUE_LENGTH));
      }
    }
  }
```

**Required import additions** (most are already present via `import java.util.*` but verify):
```java
import com.databricks.jdbc.exception.DatabricksValidationException;
import java.util.regex.Pattern;
```

Note: `DatabricksPreparedStatement extends DatabricksStatement`, so it inherits all query tag
functionality automatically. No changes needed to `DatabricksPreparedStatement`.

### 7. `src/main/java/com/databricks/jdbc/model/client/sqlexec/ExecuteStatementRequest.java`

#### 7a. Add field (after the existing `resultCompression` field)

```java
  @JsonProperty("query_tags")
  private List<QueryTag> queryTags;
```

**Required import:**
```java
import java.util.List;
```

#### 7b. Add getter and fluent setter

```java
  public List<QueryTag> getQueryTags() {
    return queryTags;
  }

  public ExecuteStatementRequest setQueryTags(List<QueryTag> queryTags) {
    this.queryTags = queryTags;
    return this;
  }
```

#### 7c. Update `equals()` — add to the existing chain

Add `&& Objects.equals(this.queryTags, other.queryTags)` to the equals comparison chain.

#### 7d. Update `hashCode()` — add to the existing `Objects.hash()` call

Add `queryTags` to the argument list of the existing `Objects.hash(...)` call.

#### 7e. Update `toString()` — add to the existing `ToStringer` chain

Add `.add("queryTags", queryTags)` to the `ToStringer` builder chain.

Jackson's `NON_NULL` inclusion (configured on the SDK's `ApiClient`) will omit the `query_tags`
field from serialized JSON when the value is `null`, so no tags are sent when not set.

### 8. `src/main/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClient.java`

In the `getRequest()` method (line 604), add the following block **after** the
`if (maxRows > 0)` block (line 647-649) and **before** `return request;` (line 650):

```java
    if (parentStatement != null) {
      Map<String, String> queryTags = parentStatement.getQueryTags();
      if (queryTags != null && !queryTags.isEmpty()) {
        List<QueryTag> queryTagList =
            queryTags.entrySet().stream()
                .map(e -> new QueryTag(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        request.setQueryTags(queryTagList);
      }
    }
```

**Required imports:**
```java
import com.databricks.jdbc.model.client.sqlexec.QueryTag;
```

This follows the same pattern as `maxRows` being read from `parentStatement` at line 620-621:
```java
long maxRows =
    (parentStatement == null) ? DEFAULT_RESULT_ROW_LIMIT : parentStatement.getMaxRows();
```

### 9. `src/main/java/com/databricks/jdbc/dbclient/impl/thrift/DatabricksThriftServiceClient.java`

In the `getRequest()` method (line 199), add the following block **after** the
`maxRows` block (lines 246-252, after `request.setResultRowLimit(maxRows)`) and **before** the
`runAsync` block (line 254):

```java
    if (parentStatement != null) {
      Map<String, String> queryTags = parentStatement.getQueryTags();
      if (queryTags != null && !queryTags.isEmpty()) {
        String queryTagsStr =
            queryTags.entrySet().stream()
                .map(
                    e ->
                        e.getValue() != null
                            ? e.getKey() + ":" + e.getValue()
                            : e.getKey())
                .collect(Collectors.joining(","));
        Map<String, String> confOverlay = new HashMap<>();
        confOverlay.put(QUERY_TAGS, queryTagsStr);
        request.setConfOverlay(confOverlay);
      }
    }
```

This uses `DatabricksJdbcConstants.QUERY_TAGS` (already defined as `"query_tags"` at line 112
of `DatabricksJdbcConstants.java`). The string format `"key1:value1,key2:value2"` matches the
session-level query tags format exactly.

**Required imports** (verify these aren't already present):
```java
import java.util.HashMap;
import java.util.stream.Collectors;
```

`QUERY_TAGS` is already available via the static import of `DatabricksJdbcConstants.*`.

---

## Tests to Add to Existing Files

### 10. `src/test/java/com/databricks/jdbc/api/impl/DatabricksStatementTest.java`

Add these test methods to the existing test class. Follow the existing inline fixture pattern
(create `DatabricksConnectionContext`, `DatabricksConnection`, `DatabricksStatement` per test).

```java
  @Test
  public void testSetAndGetQueryTags() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    Map<String, String> tags = new HashMap<>();
    tags.put("team", "marketing");
    tags.put("dashboard", "abc123");
    statement.setQueryTags(tags);

    Map<String, String> result = statement.getQueryTags();
    assertEquals("marketing", result.get("team"));
    assertEquals("abc123", result.get("dashboard"));
    assertEquals(2, result.size());
  }

  @Test
  public void testSetQueryTagsNull() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    Map<String, String> tags = new HashMap<>();
    tags.put("team", "marketing");
    statement.setQueryTags(tags);
    assertNotNull(statement.getQueryTags());

    statement.setQueryTags(null);
    assertNull(statement.getQueryTags());
  }

  @Test
  public void testSetQueryTagsEmptyMap() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    Map<String, String> tags = new HashMap<>();
    tags.put("team", "marketing");
    statement.setQueryTags(tags);
    assertNotNull(statement.getQueryTags());

    statement.setQueryTags(new HashMap<>());
    assertNull(statement.getQueryTags());
  }

  @Test
  public void testSetQueryTagsWithNullValue() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    Map<String, String> tags = new HashMap<>();
    tags.put("debug", null);
    statement.setQueryTags(tags);

    Map<String, String> result = statement.getQueryTags();
    assertTrue(result.containsKey("debug"));
    assertNull(result.get("debug"));
  }

  @Test
  public void testSetQueryTagsTooMany() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    Map<String, String> tags = new HashMap<>();
    for (int i = 0; i < 21; i++) {
      tags.put("key" + i, "value" + i);
    }
    assertThrows(DatabricksValidationException.class, () -> statement.setQueryTags(tags));
  }

  @Test
  public void testSetQueryTagsKeyTooLong() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    Map<String, String> tags = new HashMap<>();
    tags.put("k".repeat(129), "v");
    assertThrows(DatabricksValidationException.class, () -> statement.setQueryTags(tags));
  }

  @Test
  public void testSetQueryTagsValueTooLong() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    Map<String, String> tags = new HashMap<>();
    tags.put("k", "v".repeat(129));
    assertThrows(DatabricksValidationException.class, () -> statement.setQueryTags(tags));
  }

  @Test
  public void testSetQueryTagsInvalidKeyChars() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    for (String invalidChar : new String[]{",", ":", "-", "/", "=", "."}) {
      Map<String, String> tags = new HashMap<>();
      tags.put("key" + invalidChar + "name", "value");
      assertThrows(
          DatabricksValidationException.class,
          () -> statement.setQueryTags(tags),
          "Expected exception for invalid char: " + invalidChar);
    }
  }

  @Test
  public void testSetQueryTagsNullKey() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    Map<String, String> tags = new HashMap<>();
    tags.put(null, "value");
    assertThrows(DatabricksValidationException.class, () -> statement.setQueryTags(tags));
  }

  @Test
  public void testSetQueryTagsEmptyKey() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    Map<String, String> tags = new HashMap<>();
    tags.put("", "value");
    assertThrows(DatabricksValidationException.class, () -> statement.setQueryTags(tags));
  }

  @Test
  public void testSetQueryTagsOnClosedStatement() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);
    statement.close(true);

    Map<String, String> tags = new HashMap<>();
    tags.put("team", "marketing");
    assertThrows(DatabricksSQLException.class, () -> statement.setQueryTags(tags));
  }

  @Test
  public void testGetQueryTagsReturnsImmutableMap() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);
    DatabricksStatement statement = new DatabricksStatement(connection);

    Map<String, String> tags = new HashMap<>();
    tags.put("team", "marketing");
    statement.setQueryTags(tags);

    Map<String, String> result = statement.getQueryTags();
    assertThrows(UnsupportedOperationException.class, () -> result.put("new", "tag"));
  }
```

**Required import additions:**
```java
import com.databricks.jdbc.exception.DatabricksValidationException;
```

### 11. `src/test/java/com/databricks/jdbc/dbclient/impl/sqlexec/DatabricksSdkClientTest.java`

Add tests to verify query tags are wired through to `ExecuteStatementRequest`. The exact
approach depends on existing test patterns in this file — use `ArgumentCaptor` if the test
class uses Mockito to capture requests, or verify the returned request object if `getRequest()`
is tested directly.

```java
  @Test
  public void testExecuteStatementWithQueryTags() throws Exception {
    // Set up a statement with query tags
    // Execute statement
    // Capture the ExecuteStatementRequest sent to the API
    // Verify request.getQueryTags() contains expected QueryTag objects
    //   - QueryTag("team", "marketing")
    //   - QueryTag("dashboard", "abc123")
  }

  @Test
  public void testExecuteStatementWithoutQueryTags() throws Exception {
    // Execute statement without setting query tags
    // Capture the ExecuteStatementRequest
    // Verify request.getQueryTags() is null
  }
```

Note: The exact test implementation depends on the existing test setup in `DatabricksSdkClientTest.java`.
Examine the file to understand how requests are captured/verified before writing these tests.

---

## NEXT_CHANGELOG.md

Add the following entry under `### Added`:

```
- Added `setQueryTags(Map<String, String>)` and `getQueryTags()` methods to `IDatabricksStatement` for per-statement query tag support, enabling tracking and cost attribution at the individual statement level.
```

The full `### Added` section should read:

```markdown
### Added
- Added connection property `OAuthWebServerTimeout` to configure the OAuth browser authentication timeout for U2M (user-to-machine) flows, and also updated hardcoded 1-hour timeout to default 120 seconds timeout.
- Added `setQueryTags(Map<String, String>)` and `getQueryTags()` methods to `IDatabricksStatement` for per-statement query tag support, enabling tracking and cost attribution at the individual statement level.
```

---

## Implementation Order

1. Create `QueryTag.java` + `QueryTagTest.java` — pure POJO, no dependencies
2. Modify `ExecuteStatementRequest.java` + create `ExecuteStatementRequestTest.java` — add new field
3. Modify `IDatabricksStatementInternal.java` — add `getQueryTags()` to internal interface
4. Modify `IDatabricksStatement.java` — add `setQueryTags()` + `getQueryTags()` to public interface
5. Modify `DatabricksStatement.java` — implement both interfaces, add validation + field + tests
6. Modify `DatabricksSdkClient.java` — wire query tags into SEA `getRequest()` + add tests
7. Modify `DatabricksThriftServiceClient.java` — wire query tags via `confOverlay` in Thrift `getRequest()`
8. Update `NEXT_CHANGELOG.md`

---

## Verification Commands

```bash
# Build
mvn clean install -DskipTests

# Run individual test classes
mvn test -pl . -Dtest=QueryTagTest
mvn test -pl . -Dtest=ExecuteStatementRequestTest
mvn test -pl . -Dtest=DatabricksStatementTest
mvn test -pl . -Dtest=DatabricksSdkClientTest

# Format check
mvn spotless:apply

# Full test suite
mvn test
```

---

## Key Design Decisions

1. **Tags stored on `DatabricksStatement`, not passed per-`execute()`** — follows the JDBC
   pattern where statement properties (timeout, maxRows, fetchSize) are set on the Statement
   object before execution.

2. **Validation on set, not on execute** — fail fast with clear error messages at
   `setQueryTags()` time.

3. **Null/empty map = no tags** — `setQueryTags(null)` or `setQueryTags(emptyMap)` clears
   previously set tags.

4. **SEA path uses native `query_tags` field** — structured `QueryTag` objects matching the
   proto definition on `ExecuteStatementRequest`.

5. **Thrift path uses `confOverlay`** — `TExecuteStatementReq.confOverlay` with key
   `"query_tags"` and value in `"key1:value1,key2:value2"` format (matches session-level format).

6. **No driver-side merging with session tags** — session-level and statement-level tags are
   independent. The server handles any merging semantics.

7. **`PreparedStatement` inherits automatically** — `DatabricksPreparedStatement extends
   DatabricksStatement`, so all query tag functionality is inherited with no additional changes.

8. **Immutable storage** — tags stored as `Collections.unmodifiableMap(new HashMap<>(input))`
   to prevent external mutation after setting.

---

## Existing Code References

| What                          | Where                                                                    |
|-------------------------------|--------------------------------------------------------------------------|
| `QUERY_TAGS` constant         | `DatabricksJdbcConstants.java:112` — `"query_tags"`                      |
| `maxRows` read pattern (SEA)  | `DatabricksSdkClient.java:620-621` — `parentStatement.getMaxRows()`      |
| `maxRows` read pattern (Thrift)| `DatabricksThriftServiceClient.java:~250` — `parentStatement.getMaxRows()` |
| Session-level tags flow       | `DatabricksConnectionContext.getSessionConfigs()` → session creation     |
| POJO pattern                  | `PositionalStatementParameterListItem.java` — `@JsonProperty` + fluent   |
| Exception pattern             | `DatabricksValidationException(String reason)` constructor               |
| Test pattern                  | `DatabricksStatementTest.java` — JUnit 5 + Mockito, inline fixtures      |
| `checkIfClosed()` guard       | `DatabricksStatement.java` — called at top of every public method        |
| `confOverlay` on Thrift       | `TExecuteStatementReq` field 3 — `optional map<string, string>`          |
