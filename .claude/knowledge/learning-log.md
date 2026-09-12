# Engineer-bot learning log

This file is the canonical, human-gated knowledge log shared by two consumers
configured in `.bot/config.yaml`:

- `retrospective.log_path` — the retrospective flow APPENDS a dated section here
  when it extracts a durable, reusable learning (via a human-gated rolling PR).
- `author.knowledge_log` — the author phase READS this file so fixes benefit from
  what has been learned. Seeded here so the read path is never a missing file.

No learnings have been recorded yet. Dated sections are appended below by the
retrospective flow.
## Entries

### 2026-08-14: learnings since 2026-08-13T17:41:38Z
- **Context:** Author run #31744986912 fixed a parameter-bind bug where `DatabricksTypeUtil.getDatabricksTypeFromSQLType` collapsed both `Types.FLOAT` and `Types.REAL` to the 4-byte Databricks `FLOAT` wire type, silently narrowing FLOAT binds.
  **Rule:** Per the JDBC spec (Appendix B type table), `Types.FLOAT` is a synonym for `DOUBLE` (8-byte double precision, Java `double`) and must map to the 8-byte wire type; only `Types.REAL` is 4-byte single precision (Java `float`) — never collapse FLOAT and REAL to the same Databricks type, and treat the declared SQL target type (not the bound value's native type) as what drives the wire type.
- **Context:** In author run #31744986912 the read-only diagnosis/plan phase spent a turn calling `edit_file` to create a test file (turn 44), only to discover editing tools are unavailable in that phase (turn 46) — the deliverable is the structured plan, not code.
  **Rule:** The diagnosis/plan phase is read-only; don't attempt `edit_file`/write operations there. Defer all file creation and edits to the author_tests/fix phases and keep the diagnosis phase to reads, greps, and the structured plan output.

### 2026-08-18: learnings since 2026-08-17T17:33:39Z
- **Context:** PR #1582 wired `merge_group` triggers into required-check workflows (checkNextChangelog, releaseFreeze, prCheck) for a GitHub merge queue.
  **Rule:** For a required status check to stay valid under a GitHub merge queue, register the workflow on the `merge_group` event; PR-semantic jobs that read `github.event.pull_request.*` should self-skip in the queue with `if: github.event_name != 'merge_group'` — a skipped required check counts as a pass under branch protection, so the queue is not blocked while the gate stays enforced at PR time.
- **Context:** In PR #1582 the engineer-bot re-posted the same "NEEDS HUMAN DECISION / blocked" verdict on successive replies of one review thread because the reviewer's finding required editing `.github/workflows/*.yml`, a non-writable path in this environment.
  **Rule:** When a review finding targets a path the environment cannot write (e.g. `.github/workflows/*`), state once that it is agreed-but-not-actionable-here and stop; do not re-analyze or re-post a blocked verdict on each subsequent thread reply.

### 2026-08-20: learnings since 2026-08-19T17:33:03Z
- **Context:** PR #1629 added telemetry error-classification guardrails to CLAUDE.md and the PR template, covering any error emitted by the JDBC driver.
  **Rule:** When adding or changing a driver-emitted error, use `DatabricksDriverErrorCode` (reuse a matching code or add a uniquely-numbered enum value), add a test asserting the emitted error name and numeric code, and record its driver/server/user classification in the maintainers' telemetry taxonomy — never infer the classification from the error name alone.

### 2026-08-22: learnings since 2026-08-21T17:33:33Z
- **Context:** PR #1652 fixed connections failing when a parameter appeared in both the JDBC URL and the `Properties` object; the root cause was Guava's `ImmutableMap.Builder.build()`, which throws on duplicate keys. The fix inserts properties first, then URL params, and calls `buildKeepingLast()` so URL wins.
  **Rule:** When merging config from multiple sources into a Guava `ImmutableMap.Builder`, `build()` throws on duplicate keys — use `buildKeepingLast()` and insert entries in ascending precedence order (lowest-priority source first) so the highest-priority source wins.
- **Context:** PR #1621's new `BatchParameterSet` normalizes JDBC's 1-based parameter indexes to **zero-based** wire ordinals (`cardinal = index - 1`), while the existing SEA/Thrift path (`DatabricksPreparedStatement.setObject` → `mapToParameterListItem.setOrdinal`) forwards the raw **1-based** index. A reviewer flagged the off-by-one; the author confirmed the native-batch backend contract expects zero-based ordinals (first param = 0), whereas the current non-batch backend ignores the ordinal and relies on positional order.
  **Rule:** Parameter ordinals diverge by execution path — native batching expects 0-based ordinals while the existing SEA/Thrift path sends 1-based JDBC indexes (tolerated only because that backend uses positional order); when wiring native batching through shared `setOrdinal` plumbing, verify the ordinal base against the backend contract and cover both paths with request-capture tests to avoid an off-by-one.

### 2026-08-26: learnings since 2026-08-25T17:34:14Z
- **Context:** PR #1663 bumped `httpcore5`/`httpcore5-h2` to 5.4.3 while leaving `httpclient5` at 5.5.2 (tested against the httpcore5 5.3.x branch); a reviewer flagged the version skew and the fix was to bump httpclient5 to 5.6.3 so all three came from one published, mutually-tested dependency set. The original test plan built with `-Dmaven.test.skip=true`.
  **Rule:** When bumping one library in a tightly-coupled family (e.g. httpclient5/httpcore5), move the whole set to a single published-compatible combination rather than pinning individual minor versions — a lone minor bump under a dependent tested against a different branch surfaces as a runtime NoSuchMethodError/LinkageError, not a compile error, and must be validated with the HTTP-path/integration tests (never a skip-tests build).
- **Context:** In PR #1641 the Thrift-native metadata error-propagation guard keyed off the connection-level request flag `isThriftNativeMetadataRequested()` rather than the response manifest `resultSet.isThriftNativeMetadataResult()`; because the exception is thrown before any result is available, enabling the opt-in flag silently regressed the object-not-found / ALL-CATALOGS-parse-error → empty-ResultSet fallbacks even against legacy servers that ignore the feature header.
  **Rule:** Gate behavior changes for an opt-in feature on whether the feature actually took effect (server/response manifest), not on whether it was merely requested — otherwise enabling the flag changes semantics against servers that don't support it, breaking JDBC contract behavior (e.g. getTables/getColumns returning empty for a missing object).
- **Context:** In PR #1641 the native error-propagation re-throw guard was added to `listTables`/`listColumns` but not to the sibling `listFunctions`/`listPrimaryKeys`/`listImportedKeys`/`listCrossReferences`, which kept swallowing object-not-found/parse errors into empty results — reviewers repeatedly flagged the asymmetry as reading like an oversight.
  **Rule:** When changing error-handling (or any behavioral) policy in one of a family of near-identical operations, apply it to all of them or add a comment explaining the deliberate divergence; partial application across parallel metadata paths reads as a bug and yields inconsistent contract behavior.
- **Context:** In PR #1641 `copyThriftNativeMetadataRows` copied server native rows purely positionally (`getObject(1..columnCount)`) and downstream consumers relied on fixed indices (`row.get(0)`, `CROSS_REFERENCE_COLUMNS.indexOf(...)`), with no validation of column count or names against the expected `*_COLUMNS` lists.
  **Rule:** When consuming a wire/native result positionally against a server-controlled schema, validate column count (and ideally names) against the expected column list, or map by name — otherwise schema drift silently mis-filters/mislabels rows instead of failing loudly.
- **Context:** In PR #1641 `testThriftNativeFormattingMatchesRawThriftBuilder` compared the `DatabricksResultSet`-taking overload against the `List<List<Object>>`-taking overload, but the native branch simply forwards to that same row builder, so the assertion was tautological and never exercised the real risk (native JDBC-ordered rows normalized differently than a legacy SEA result going through `getRows`+adapters).
  **Rule:** A test that compares two code paths which delegate to the same underlying method proves only delegation, not equivalence; to guard a real divergence risk, assert the new path's output against an independently-computed reference (e.g. a genuine legacy-adapter result), not another wrapper over the same builder.

### 2026-08-27: learnings since 2026-08-26T18:05:01Z
- **Context:** PR #1659 fixed a `NullPointerException` in `DatabricksArray.convertElements` when materializing arrays of nested complex types (ARRAY/MAP/STRUCT) that contain literal `null` elements; the reviewer noted the initial fix added a per-branch null check but missed the STRUCT branch, which still NPE'd while building its error message.
  **Rule:** Arrays can legally hold `null` for ANY element type, so when converting/materializing nested complex types (ARRAY, MAP, STRUCT), handle `element == null` once at the top of the loop before type-specific dispatch — don't scatter per-branch null checks that are easy to miss for one type.

### 2026-09-05: learnings since 2026-09-04T17:26:14Z
- **Context:** PR #1672 gated `UseBoundedSeaApi`/`EnableThriftNativeMetadata` defaults on the server-side `enableSqlExecForJdbc` rollout flag via `resolveFeatureFlag`; a reviewer asked whether defaulting the param to `1` makes the server flag redundant.
  **Rule:** When defaulting a client feature-flag param to `1` behind a server rollout flag, resolve in priority order — explicit connection param wins; otherwise if the default is `1` defer to the server flag's value; otherwise return false — so a `1` default enables server-controlled rollout rather than forcing the feature on.
- **Context:** In PR #1672's `NEXT_CHANGELOG.md`, reviewers flagged that a public release note referenced the internal server flag `enableSqlExecForJdbc` and lacked opt-out guidance for a newly default-enabled param.
  **Rule:** Public-facing changelog entries for a newly-introduced or default-enabled connection param must describe what the param does and how to opt out, and must not expose internal server-side flag names.

### 2026-09-08: learnings since 2026-09-07T17:26:57Z
- **Context:** PR #1670 fixed later logging-enabled JDBC connections being unable to emit logs because an earlier `LogLevel=OFF` connection had already flipped the one-time `isLoggerInitialized` static guard (and installed no usable handler). The fix returns from the OFF/no-op path *without* setting the guard, and also leaves it unset when handler installation throws (`testFailedInitializationCanBeRetried`), so a later capable caller can still initialize.
  **Rule:** Set a one-time initialization guard (static `isInitialized` flag / singleton) only after a fully-functional configuration is actually installed — never on a disabled/no-op early path or before an operation that can throw — otherwise the first weak or failed caller permanently poisons initialization for every later caller.
- **Context:** In PR #1670 a reviewer flagged that detecting the "logging off" case should compare `level.intValue() == Level.OFF.intValue()` rather than by object reference/equality, since `java.util.logging.Level` allows distinct instances that share the same integer value.
  **Rule:** Compare `java.util.logging.Level` (and similar level/enum-like value objects) by `intValue()`, not by `==` reference or `.equals()`, because semantically-equal levels can be different object instances.

### 2026-09-10: learnings since 2026-09-09T17:27:44Z
- **Context:** PR #1680 fixed `DatabaseMetaData.getTypeInfo()` returning the `INTERVAL` row out of `DATA_TYPE` order; the fix reordered the static row array and added a test asserting rows are sorted by `DATA_TYPE`.
  **Rule:** JDBC `DatabaseMetaData` metadata result sets carry spec-mandated ordering contracts (e.g. `getTypeInfo()` MUST return rows ordered by `DATA_TYPE`) — when adding or editing rows in a hardcoded metadata table, preserve the required sort order and guard it with an ordering assertion, since column values alone won't reveal the violation.

### 2026-09-12: learnings since 2026-09-11T17:27:24Z
- **Context:** In PR #1683 the Reyden auto-recovery double-failure test asserted only on `getMessage()` and `assertNotNull(getCause())`, but the production code preserves the original error via `addSuppressed(e)` while the cause is a *different* exception — so the test would still pass if `addSuppressed` were deleted, leaving the "neither error chain is lost" guarantee untested.
  **Rule:** When code preserves an original error as a *suppressed* exception (`addSuppressed`), the test must inspect `getSuppressed()` (and assert its SQLSTATE/identifying message), not just `getCause()`/`getMessage()` — otherwise the suppression is effectively unverified.
- **Context:** PR #1683's `ReydenWarehouseCache` built its `(host, warehouse_id)` cache key with `host.toLowerCase()` using the default JVM locale, which folds `I`/`i` unexpectedly on a Turkish-locale JVM.
  **Rule:** Always pass `Locale.ROOT` to `String.toLowerCase()`/`toUpperCase()` when normalizing identifiers, hostnames, or cache keys — locale-default case folding is a latent multi-locale bug.
- **Context:** PR #1683's cache read `System.currentTimeMillis()` directly against a hard-coded 6h TTL, leaving the entire expiry/eviction path untestable without reflection or real sleeps; the fix injected a `LongSupplier` clock and TTL override via a package-private constructor.
  **Rule:** Make the time source (and TTL) injectable for any TTL/expiry/eviction logic so the expiry branches can be driven deterministically in tests — code that reads the wall clock directly leaves its core time-dependent behavior unverified.
- **Context:** In PR #1683 the fallback recovery only re-attached the original Thrift `KP001` as suppressed when the SEA fallback threw `DatabricksSQLException`; an unchecked exception (SDK `RuntimeException`/NPE) from the fallback path would escape and drop the original error context entirely.
  **Rule:** When a fallback/recovery path wraps its failure to preserve the original error, catch broadly enough (or wrap) to also cover unchecked exceptions from the fallback — a narrow typed catch silently loses the original error on unexpected failure modes.
- **Context:** PR #1683's Reyden auto-recovery silently switched connections that had forced Thrift-only metadata (`UseQueryForMetadata=0` / `TreatMetadataCatalogNameAsPattern=1`) over to SEA, which serves metadata differently; the reviewer had it emit a `LOGGER.warn` so the behavior change is observable rather than a debug-only line.
  **Rule:** When automatic recovery/fallback overrides an explicit user configuration, log it at WARN so the otherwise-silent behavior change is observable — don't bury a semantics change at debug level.
