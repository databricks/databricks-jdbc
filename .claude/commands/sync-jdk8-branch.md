---
description: Sync the jdk-8 branch with the latest main branch and apply all JDK 8 compatibility transformations.
---

### User Input

```text
$ARGUMENTS
```

Optional: a specific commit SHA, PR number, or branch name to sync from. If empty, sync from the tip of `main`.

## Goal

Keep the `jdk-8` branch up to date with `main` while ensuring full Java 8 compatibility. This involves merging or cherry-picking changes from `main`, then applying a fixed set of transformations that strip out Java 9+ APIs, downgrade incompatible dependencies, and remove test infrastructure that does not work on JDK 8.

---

## Steps

### Step 1: Set Up Local Branches

1. Verify the active GitHub account is the non-EMU account required for this repo:
   ```bash
   gh auth status
   ```
   Switch if needed: `gh auth switch --user <account>`

2. Fetch the latest state of both branches:
   ```bash
   git fetch origin main jdk-8
   ```

3. Check out the `jdk-8` branch:
   ```bash
   git checkout jdk-8
   ```

4. Show the user the commits on `main` that are not yet in `jdk-8`:
   ```bash
   git log jdk-8..origin/main --oneline
   ```
   Summarise the list and ask the user to confirm before proceeding:
   - If `$ARGUMENTS` specifies a commit or range, apply only those.
   - Otherwise, merge the full `origin/main` tip.

---

### Step 2: Merge `main` into `jdk-8`

Merge (or cherry-pick as appropriate) the confirmed commits:

```bash
git merge origin/main --no-commit --no-ff
```

Do **not** commit yet — the merge result needs the compatibility transformations applied first.

If there are merge conflicts, resolve them and stage the resolutions before continuing.

---

### Step 3: Apply JDK 8 Compatibility Transformations

Apply **all** of the following changes to the merged result. Read each file before editing it.

---

#### 3a. Root `pom.xml` — compiler target

Change `maven.compiler.source` and `maven.compiler.target` from `11` to `1.8`:
```xml
<maven.compiler.source>1.8</maven.compiler.source>
<maven.compiler.target>1.8</maven.compiler.target>
```

---

#### 3b. Root `pom.xml` — JDK 8 spotless-skip profile

Ensure the following profile exists inside `<profiles>`. Add it if it was lost during the merge:
```xml
<profile>
  <!-- Skip spotless on JDK 8: spotless-maven-plugin 2.39.0 requires Java 11+ -->
  <id>jdk8</id>
  <activation>
    <jdk>1.8</jdk>
  </activation>
  <properties>
    <spotless.skip>true</spotless.skip>
  </properties>
</profile>
```

---

#### 3c. Root `pom.xml` — downgrade incompatible dependency versions

Update the following `<properties>` entries. Only change these if the value differs from the target:

| Property | Required jdk-8 value | Reason |
|---|---|---|
| `arrow.version` | `13.0.0` | Arrow 14+ requires Java 11 |
| `mockito.version` | `4.11.0` | Mockito 5.x requires Java 11 |
| `nimbusjose.version` | `9.47` | nimbus-jose-jwt 10.x requires Java 11 |

Remove `wiremock.version` if it is present and fakeservice tests have been removed (see §3g).

---

#### 3d. Root `pom.xml` — remove Arrow patched-class spotless exclusions

In the spotless plugin `<configuration>`, remove the `<excludes>` block for patched Arrow classes if it was introduced from `main`:
```xml
<!-- REMOVE if present: -->
<excludes>
  <exclude>**/MemoryUtil.java</exclude>
  <exclude>**/ArrowBuf.java</exclude>
  <exclude>**/DecimalUtility.java</exclude>
</excludes>
```

---

#### 3e. `jdbc-core/pom.xml` — remove `--add-opens` from surefire and exec plugin

`--add-opens=java.base/java.nio=ALL-UNNAMED` is a Java 9+ module flag and is invalid on JDK 8. Remove it everywhere it appears — in both the surefire `<argLine>` and the exec-maven-plugin `<arguments>` block.

Do **not** add any `<javaVersion>`-based conditional logic. The jdk-8 branch targets only JDK 8. The surefire default `<argLine>` must be:
```xml
<argLine>
  @{argLine}
  -Xmx5g
  -Dnet.bytebuddy.experimental=true
</argLine>
```

---

#### 3f. `jdbc-core/pom.xml` — remove Arrow-specific profiles and JaCoCo exclusions

Delete these two profiles entirely if they exist:
- `jdk17-NioNotOpen`
- `jdk21-NioNotOpen`

In the JaCoCo `<configuration><excludes>` block, remove the entries for patched Arrow and custom Arrow classes:
```
org/apache/arrow/memory/util/MemoryUtil*
org/apache/arrow/memory/ArrowBuf*
org/apache/arrow/vector/util/DecimalUtility*
org/apache/arrow/memory/DatabricksAllocationReservation*
org/apache/arrow/memory/DatabricksBufferAllocator*
org/apache/arrow/memory/DatabricksReferenceManager*
**/DatabricksArrowBuf*
```

Also remove the `<groups>!Jvm17PlusAndArrowToNioReflectionDisabled</groups>` filter from the `local` profile's surefire configuration.

---

#### 3g. `jdbc-core/pom.xml` — remove WireMock dependency and add fakeservice/e2e excludes

Remove the `wiremock` test dependency block entirely.

Ensure the surefire `<excludes>` list contains:
```xml
<exclude>**/integration/fakeservice/**/*.java</exclude>
<exclude>**/integration/e2e/**/*.java</exclude>
```

---

#### 3h. Source code — remove JDBC 4.3 `ShardingKey` methods

`java.sql.ShardingKey` was introduced in Java 9 (JDBC 4.3) and does not exist in the JDK 8 `java.sql` package.

In `DatabricksConnection.java`, remove all four override methods and their imports:
- `setShardingKeyIfValid(ShardingKey, ShardingKey, int)`
- `setShardingKeyIfValid(ShardingKey, int)`
- `setShardingKey(ShardingKey, ShardingKey)`
- `setShardingKey(ShardingKey)`

Also remove the `import java.sql.ShardingKey;` line.

Check for any other JDBC 4.3 (Java 9+) APIs introduced from `main` — such as methods taking `java.sql.ConnectionBuilder` or `java.sql.PooledConnectionBuilder` — and remove those as well.

---

#### 3i. Source code — remove all Arrow-specific code

All of the following were introduced in PR #1243 ("Arrow patch to circumvent Arrow issues with JDK 16+"). The patch exists solely to work around NIO module restrictions introduced in JDK 16+ — it is not relevant to JDK 8, which predates the Java module system entirely. Do not carry these files into the jdk-8 branch. Arrow 13.0.0 (the last JDK 8 compatible version) works correctly without any patching.

Delete these source files if present:

**Patched Arrow internals** (forked from Arrow source to work around JDK 17+ NIO restrictions):
- `src/main/java/org/apache/arrow/memory/util/MemoryUtil.java`
- `src/main/java/org/apache/arrow/memory/ArrowBuf.java`
- `src/main/java/org/apache/arrow/vector/util/DecimalUtility.java`

**Databricks-custom Arrow allocator classes** (depend on the patched internals above):
- `src/main/java/org/apache/arrow/memory/DatabricksAllocationReservation.java`
- `src/main/java/org/apache/arrow/memory/DatabricksArrowBuf.java`
- `src/main/java/org/apache/arrow/memory/DatabricksBufferAllocator.java`
- `src/main/java/org/apache/arrow/memory/DatabricksReferenceManager.java`
- `src/main/java/org/apache/arrow/memory/DatabricksReferenceManagerNOOP.java`

For `src/main/java/com/databricks/jdbc/api/impl/arrow/ArrowBufferAllocator.java` and `AbstractArrowResultChunk.java` — these were modified in PR #1243 to use the custom allocator classes above. Revert any such changes so they use the standard Arrow 13.0.0 APIs only.

---

#### 3j. Test code — remove fakeservice and e2e tests

Delete these directories entirely if they were added or modified in the merge:
- `src/test/java/com/databricks/jdbc/integration/fakeservice/`
- `src/test/java/com/databricks/jdbc/integration/e2e/`

Also remove the associated WireMock test resource directories if no other tests depend on them:
- `src/test/resources/sqlexecapi/`
- `src/test/resources/thriftserverapi/`
- `src/test/resources/cloudfetchapi/`

---

#### 3k. Test code — remove all Arrow patch tests

All of the following test files were introduced in PR #1243 alongside the Arrow patch code. Since the patch code is removed, these tests must be deleted:

**Arrow allocator manager tests** (under `src/test/java/com/databricks/jdbc/api/impl/arrow/`):
- `ArrowBufferAllocatorNettyManagerTest.java`
- `ArrowBufferAllocatorUnsafeManagerTest.java`
- `ArrowBufferAllocatorUnknownManagerTest.java`
- `ArrowBufferAllocatorTest.java`

**Databricks Arrow patch tests** (under `src/test/java/org/apache/arrow/memory/`):
- `AbstractDatabricksArrowPatchTypesTest.java`
- `ArrowParsingBenchmark.java`
- `DatabricksAllocationReservationTest.java`
- `DatabricksArrowBufTest.java`
- `DatabricksArrowPatchBinaryStringTypesTest.java`
- `DatabricksArrowPatchComplexTypesTest.java`
- `DatabricksArrowPatchMemoryUsageTest.java`
- `DatabricksArrowPatchNumericTypesTest.java`
- `DatabricksArrowPatchTemporalTypesTest.java`
- `DatabricksArrowPatchTest.java`
- `DatabricksBufferAllocatorTest.java`
- `DatabricksReferenceManagerNOOPTest.java`
- `DatabricksReferenceManagerTest.java`

**Arrow test resources**:
- `src/test/resources/arrow/` (entire directory)

Also remove any test methods or classes annotated with `@Tag("Jvm17PlusAndArrowToNioReflectionDisabled")`.

---

#### 3l. Full dependency audit — every dependency in every `pom.xml`

**Every single dependency across all `pom.xml` files** (root, `jdbc-core`, `assembly-uber`, `assembly-thin`, `test-assembly-uber`, `test-assembly-thin`) must be verified for JDK 8 compatibility after the merge. Do not assume a dependency is safe just because it was already present — a version bump from `main` may have moved it to a Java 9+ baseline.

For each dependency, check the version's minimum Java requirement via:
1. The library's Maven Central page or release notes (search `<groupId>:<artifactId> <version> java requirement`).
2. The JAR's `MANIFEST.MF` `Bundle-RequiredExecutionEnvironment` or `Require-Java` field if needed.

Apply the following known compatibility rules:

| Dependency | Safe versions for JDK 8 | Versions that require Java 11+ |
|---|---|---|
| `org.apache.arrow:arrow-*` | ≤ 13.x | 14.0.0+ |
| `org.mockito:mockito-*` | ≤ 4.x | 5.x+ |
| `com.nimbusds:nimbus-jose-jwt` | ≤ 9.x | 10.x+ |
| `org.wiremock:wiremock` | — (remove with fakeservice tests) | 3.x requires Java 11 |
| `com.diffplug.spotless:spotless-maven-plugin` | — (skip via profile, do not run) | 2.28.0+ requires Java 11 |
| `com.google.guava:guava` | Any `-jre` variant (means JRE 8+) | `-android` variant is also fine |
| `org.bouncycastle:bcprov-jdk18on` | All versions (`jdk18on` = JDK 1.8+) | n/a |
| `org.bouncycastle:bcpkix-jdk18on` | All versions (`jdk18on` = JDK 1.8+) | n/a |
| `com.fasterxml.jackson.*` | 2.x (all) | n/a |
| `org.slf4j:slf4j-*` | 2.x (all) | n/a |
| `io.grpc:grpc-*` | 1.x (all) | n/a |
| `org.apache.thrift:libthrift` | 0.x (all) | n/a |
| `io.github.resilience4j:*` | 1.x (all) | n/a |
| `org.immutables:value` | 2.x (all) | n/a |
| `org.junit.jupiter:*` | 5.x (all) | n/a |
| `org.apache.commons:commons-configuration2` | 2.x (all) | n/a |
| `commons-io:commons-io` | 2.x (all) | n/a |
| `org.apache.commons:commons-lang3` | 3.x (all) | n/a |
| `org.apache.httpcomponents:httpclient` | 4.x (all) | n/a |
| `org.apache.httpcomponents.client5:httpclient5` | 5.x (all) | n/a |
| `org.apache.httpcomponents.core5:httpcore5` | 5.x (all) | n/a |
| `org.lz4:lz4-java` | 1.x (all) | n/a |
| `io.netty:netty-*` | 4.x (all) | n/a |
| `org.locationtech.jts:jts-core` | 1.x (all) | n/a |
| `org.openjdk.jmh:jmh-*` | 1.x (all) | n/a |
| `com.databricks:databricks-sdk-java` | Verify each version bump | Check SDK release notes |
| `jakarta.annotation:jakarta.annotation-api` | ≤ 1.x | 2.x requires Java 11 |

**Rules for any dependency not in the table above:**
- Do not accept any version that declares `Bundle-RequiredExecutionEnvironment: JavaSE-11` or higher.
- Do not use artifacts with classifier variants like `-jre11`, `-jre17`, `-module`, or `-java11`.
- If a library bumped its minimum Java requirement in a new version pulled from `main`, pin it to the last version that supports Java 8 and add it to the table above in a comment.
- If there is no Java 8 compatible version, **stop and notify the user** before taking any action. Present the options clearly (e.g., pin to an older compatible version, swap for an alternative library, remove the dependency and its usages) and let the user decide the path forward. Do not silently remove or replace anything in this case.

---

### Step 4: Verify the Build on JDK 8

Run a clean build to confirm no compilation errors on JDK 8:

```bash
mvn clean install -DskipTests
```

Then run the unit tests:

```bash
mvn clean test
```

- Spotless will be auto-skipped via the `jdk8` Maven profile (active when running on JDK 1.8).
- Fix any compilation errors caused by residual Java 9+ API usage before proceeding.
- All tests must pass before creating a PR.

---

### Step 5: Create the PR

1. Stage all changes and commit with DCO sign-off:
   ```bash
   git add -p   # review changes before staging
   git commit -s -m "Sync jdk-8 branch with main (<short description of included changes>)"
   ```

2. Push to the remote `jdk-8` branch:
   ```bash
   git push origin jdk-8
   ```

3. Open a PR targeting `jdk-8` (not `main`):
   ```bash
   gh pr create --base jdk-8 --title "Sync jdk-8 with main: <short description>" --body "..."
   ```
   PR body must include:
   - Summary of commits synced from `main`
   - List of JDK 8 compatibility transformations applied
   - `NO_CHANGELOG=true` (sync/maintenance PRs do not need a changelog entry unless they include a user-visible fix)

4. Share the PR URL with the user.

---

## Important Notes

- **Never add `--add-opens`, `--add-exports`, or any JPMS flags** — they are invalid on JDK 8.
- **Never add branching logic for JDK 9, 11, 17, or 21** in the jdk-8 branch. The branch targets JDK 8 only.
- **Do not run `mvn spotless:apply` on JDK 8** — it will fail. The `jdk8` Maven profile skips it automatically.
  - TODO: Investigate whether an older version of `spotless-maven-plugin` (pre-2.28.0) supports JDK 8 and can enforce formatting on the jdk-8 branch instead of skipping it entirely.
- **Do not add JDBC 4.3+ APIs** (`ShardingKey`, `ConnectionBuilder`, `PooledConnectionBuilder`) — they require Java 9+.
- Keep changes minimal — only apply the transformations listed above. Do not refactor or "improve" code beyond what is required for JDK 8 compatibility.
