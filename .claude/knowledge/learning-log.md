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
