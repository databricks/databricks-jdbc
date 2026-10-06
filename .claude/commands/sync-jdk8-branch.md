---
description: Sync the jdk-8 branch with the latest main branch and apply all JDK 8 compatibility transformations.
---

### User Input

```text
$ARGUMENTS
```

Optional: a specific commit SHA or range to sync. If empty, sync from the tip of `main`.

## Goal

Merge `main` into `jdk-8` and apply all JDK 8 compatibility transformations. Confirm with the user at each decision point before making irreversible changes.

---

## Steps

### Step 1: Set Up

1. Verify the non-EMU GitHub account is active: `gh auth status`. Switch if needed.
2. `git fetch origin main jdk-8`
3. `git checkout jdk-8`
4. Show pending commits: `git log jdk-8..origin/main --oneline`. Ask user to confirm scope before proceeding.

### Step 2: Merge

```bash
git merge origin/main --no-commit --no-ff
```

Resolve any conflicts, then continue to Step 3 before committing.

---

### Step 3: Apply JDK 8 Transformations

Read each file before editing. Apply all of the following.

#### `pom.xml` (root)

- Set `maven.compiler.source` and `maven.compiler.target` to `1.8`
- **Remove spotless entirely**: delete the `spotless-maven-plugin` plugin declaration, any `<pluginManagement>` entry for it, and any `spotless.version` property. Do NOT add a skip profile — the plugin must not exist on the jdk-8 branch at all.
  <!-- Spotless is removed entirely (not just skipped) because even loading spotless-maven-plugin 2.39.0 on JDK 8 causes CI failures regardless of skip flags. TODO: Investigate whether an older version (pre-2.28.0) supports JDK 8 so formatting can be enforced rather than absent. -->
- **Remove `org.owasp:dependency-check-maven`** plugin entirely — remove from both `<plugins>` and any `<pluginManagement>` entry. Not needed on the jdk-8 branch.
- Pin these dependency versions:

  | Property | jdk-8 value | Reason |
  |---|---|---|
  | `arrow.version` | `13.0.0` | Arrow 14+ requires Java 11 |
  | `mockito.version` | `4.11.0` | Mockito 5.x requires Java 11 |
  | `nimbusjose.version` | `9.47` | nimbus-jose-jwt 10.x requires Java 11 |

- Remove `wiremock.version` (WireMock 3.x requires Java 11; fakeservice tests are removed)
- Remove the spotless `<excludes>` block for Arrow patched classes (`MemoryUtil.java`, `ArrowBuf.java`, `DecimalUtility.java`)

#### `jdbc-core/pom.xml`

- Remove `--add-opens=java.base/java.nio=ALL-UNNAMED` from surefire `<argLine>` and exec-maven-plugin `<arguments>`. Do not add any JDK-version branching — target JDK 8 only. Final surefire `<argLine>`:
  ```xml
  <argLine>@{argLine} -Xmx5g -Dnet.bytebuddy.experimental=true</argLine>
  ```
- Delete profiles `jdk17-NioNotOpen` and `jdk21-NioNotOpen` entirely
- Remove JaCoCo exclusions for Arrow patch and custom Arrow classes (`MemoryUtil*`, `ArrowBuf*`, `DecimalUtility*`, `DatabricksAllocationReservation*`, `DatabricksBufferAllocator*`, `DatabricksReferenceManager*`, `DatabricksArrowBuf*`)
- Remove `<groups>!Jvm17PlusAndArrowToNioReflectionDisabled</groups>` from the `local` profile
- Remove the `wiremock` test dependency
- Add to surefire `<excludes>`:
  ```xml
  <exclude>**/integration/fakeservice/**/*.java</exclude>
  <exclude>**/integration/e2e/**/*.java</exclude>
  ```

#### `.github/workflows/coverageReport.yml`

Change the JDK version to 8 and lower the coverage threshold to 80%:

```yaml
- name: Set up JDK
  uses: actions/setup-java@v4
  with:
    java-version: '8'
    distribution: 'adopt'
```

Update every occurrence of `85` in the threshold check to `80`. Also remove `-Dgroups='!Jvm17PlusAndArrowToNioReflectionDisabled'` from the test run command (that tag doesn't exist on the jdk-8 branch) and add `-Dspotless.skip=true`:

```bash
mvn -pl jdbc-core clean test -Dspotless.skip=true jacoco:report
```

**Do not modify any other workflow files.** All other `.github/workflows/*.yml` files must be left exactly as merged from main.

#### Source code — Java 9+ API replacements

Scan all Java source files for APIs introduced after JDK 8 and replace them. Common patterns:

| Java 9+ (incompatible) | JDK 8 replacement |
|---|---|
| `List.of(...)` | `Arrays.asList(...)` (or `Collections.unmodifiableList(Arrays.asList(...))`) |
| `Set.of(...)` | `ImmutableSet.of(...)` (Guava) |
| `Map.of(...)` | `ImmutableMap.of(...)` (Guava) |
| `Map.entry(k, v)` | `new AbstractMap.SimpleEntry<>(k, v)` (or Guava's `Maps.immutableEntry(k, v)`) |
| `Optional.isEmpty()` | `!optional.isPresent()` |
| `String.repeat(n)` | loop or `new String(new char[n]).replace('\0', c)` |
| `InputStream.nullInputStream()` | `new ByteArrayInputStream(new byte[0])` |
| `Reader.nullReader()` | `new StringReader("")` |
| `LocalDate.EPOCH` | `LocalDate.of(1970, 1, 1)` |
| `URLDecoder.decode(str, StandardCharsets.UTF_8)` (Charset overload, Java 10+) | `URLDecoder.decode(str, StandardCharsets.UTF_8.name())` |

After replacing, verify no stragglers remain:
```bash
grep -r "List\.of\|Set\.of\|Map\.of\|Map\.entry\|Optional\.isEmpty\|\.repeat(\|nullInputStream\|nullReader\|LocalDate\.EPOCH" src/main/java/ src/test/java/
```

#### Source code — Other changes

- **Remove JDBC 4.3 methods** from `DatabricksConnection.java` — `setShardingKeyIfValid` and `setShardingKey` overloads plus the `ShardingKey` import. Also remove JDBC 4.3 test methods such as `enquoteLiteral`, `enquoteIdentifier`, and any other JDBC 4.3+ APIs (`ConnectionBuilder`, `PooledConnectionBuilder`). All require Java 9+.

- **Remove Arrow patch files** (introduced in PR #1243 for JDK 16+ NIO restrictions — not relevant to JDK 8):

  *Patched Arrow internals:*
  - `src/main/java/org/apache/arrow/memory/util/MemoryUtil.java`
  - `src/main/java/org/apache/arrow/memory/ArrowBuf.java`
  - `src/main/java/org/apache/arrow/vector/util/DecimalUtility.java`

  *Custom Arrow allocator classes:*
  - `src/main/java/org/apache/arrow/memory/DatabricksAllocationReservation.java`
  - `src/main/java/org/apache/arrow/memory/DatabricksArrowBuf.java`
  - `src/main/java/org/apache/arrow/memory/DatabricksBufferAllocator.java`
  - `src/main/java/org/apache/arrow/memory/DatabricksReferenceManager.java`
  - `src/main/java/org/apache/arrow/memory/DatabricksReferenceManagerNOOP.java`

  Revert any changes to `ArrowBufferAllocator.java` and `AbstractArrowResultChunk.java` that reference the removed custom allocator classes.

#### Test code

- Delete `src/test/java/com/databricks/jdbc/integration/fakeservice/` and `src/test/java/com/databricks/jdbc/integration/e2e/`
- Delete WireMock test resources: `src/test/resources/sqlexecapi/`, `src/test/resources/thriftserverapi/`, `src/test/resources/cloudfetchapi/`
- Delete Arrow patch tests (all introduced in PR #1243):
  - `src/test/java/com/databricks/jdbc/api/impl/arrow/ArrowBufferAllocator*Test.java` (Netty, Unsafe, Unknown, base)
  - `src/test/java/org/apache/arrow/memory/Databricks*Test.java` and `AbstractDatabricksArrowPatchTypesTest.java`, `ArrowParsingBenchmark.java`
  - `src/test/resources/arrow/` (entire directory)
- Remove any test annotated with `@Tag("Jvm17PlusAndArrowToNioReflectionDisabled")`

- **Remove `IntervalConverter` overflow test**: Delete the test case for `Duration.ofNanos(Long.MIN_VALUE)` in `IntervalConverterTest` (or wherever it lives). On JDK 8, `Duration.toNanos()` uses `Math.multiplyExact()` internally and throws `ArithmeticException` when seconds × 1,000,000,000 overflows `long`. JDK 17+ doesn't have this problem. The edge case doesn't occur in practice — remove the test rather than working around it.

- **Fix `DatabricksDriverFeatureFlagsContext` hanging test**: Any test that constructs `DatabricksDriverFeatureFlagsContext` with a fake hostname (e.g. `sample-host.cloud.databricks.com`) will hang indefinitely on JDK 8. JDK 8's HTTP client has no default connection timeout, so the TCP attempt blocks for the full OS socket timeout (minutes+). JDK 17+ fails fast because of tighter defaults.

  Fix in order of preference:
  1. Mock the HTTP client / feature-flags fetcher so no real network call is made.
  2. Pass a pre-populated flag map to the constructor instead of fetching from the server.
  3. Set an explicit connection timeout before the test runs.

  Never leave a test that makes a real outbound HTTP/TCP connection to a non-existent host on the jdk-8 branch — it will cause the CI job to time out.

#### Full dependency audit

Check **every** dependency across all `pom.xml` files for JDK 8 compatibility. A version bump from `main` can silently raise the minimum Java version.

Known incompatible versions:

| Dependency | Max safe version for JDK 8 |
|---|---|
| `org.apache.arrow:arrow-*` | 13.x |
| `org.mockito:mockito-*` | 4.x |
| `com.nimbusds:nimbus-jose-jwt` | 9.x |
| `org.wiremock:wiremock` | remove entirely |
| `com.diffplug.spotless:spotless-maven-plugin` | remove entirely (not just skip) |
| `org.owasp:dependency-check-maven` | remove entirely |
| `jakarta.annotation:jakarta.annotation-api` | 1.x (2.x requires Java 11) |

Everything else in the current `pom.xml` is JDK 8 compatible. For any new dependency not in this table: check Maven Central / release notes. If no JDK 8 compatible version exists, **stop and notify the user** with the options (pin older version, swap alternative, remove) before taking action.

---

### Step 4: Verify

```bash
mvn clean install -DskipTests   # confirm no compilation errors
mvn -pl jdbc-core test -Dspotless.skip=true
```

After a successful build, confirm the jar targets JDK 8:

```bash
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
JAR=jdbc-core/target/databricks-jdbc-core-${VERSION}.jar
javap -verbose -cp "$JAR" \
  $(jar tf "$JAR" | grep '\.class$' | head -1 | sed 's/\.class$//' | tr '/' '.') \
  | grep 'major version'
# → major version: 52  ✅  (52 = JDK 8)
```

Fix any residual Java 9+ API usage before proceeding.

---

### Step 5: Create the PR

```bash
git add -p
git commit -s -m "Sync jdk-8 branch with main (<short description>)"
git push origin jdk-8
gh pr create --base jdk-8 \
  --title "Sync jdk-8 with main: <short description>" \
  --body "..."
```

PR body: summary of synced commits, list of transformations applied, `NO_CHANGELOG=true`.

Share the PR URL with the user.

---

## Rules

- Never use `--add-opens`, `--add-exports`, or any JPMS flag — invalid on JDK 8.
- Never add branching for JDK 9, 11, 17, or 21 — the branch targets JDK 8 only.
- Never run `mvn spotless:apply` on JDK 8. Spotless is fully removed from the jdk-8 branch (no plugin declaration, no pluginManagement entry, no version property) because even loading the plugin on JDK 8 causes CI failures. Always pass `-Dspotless.skip=true` as a safety net.
  <!-- TODO: Investigate whether an older `spotless-maven-plugin` version (pre-2.28.0) supports JDK 8 so formatting can be enforced rather than absent. -->
- Never add JDBC 4.3+ APIs (`ShardingKey`, `ConnectionBuilder`, `PooledConnectionBuilder`, `enquoteLiteral`, `enquoteIdentifier`).
- Only unit tests belong in the jdk-8 branch. Never carry over fakeservice or e2e tests — fakeservice depends on WireMock 3.x (Java 11+), and e2e tests are covered by comprehensive MultiDBR testing. Remove them and their test resources entirely.
- Never leave a test that makes a real outbound HTTP/TCP connection to a non-existent host — JDK 8 will hang indefinitely without a connection timeout.
- Only modify `coverageReport.yml` in `.github/workflows/`. All other workflow files must be left exactly as merged from main.
