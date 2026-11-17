# Lessons

<!-- distilled-through: none -->

## JDBC contracts and configuration
- L-0001 (1×, log:2026-08-14): Types.FLOAT maps to DOUBLE and 8-byte wire values; only Types.REAL is single precision. The declared SQL target type drives wire typing.
- L-0002 (1×, #1652): Merge Guava immutable maps in ascending configuration precedence and use buildKeepingLast so higher-priority values replace duplicates.
- L-0003 (1×, #1621): Native batch parameter ordinals are zero-based; legacy SEA/Thrift uses one-based JDBC indexes. Capture outbound requests on both paths.
- L-0004 (2×, #1641 #1678): Gate feature-specific request fields and behavior on the feature taking effect in the server response, preserving behavior for connections that did not opt in.
- L-0005 (1×, #1641): Apply behavioral and error-policy changes to every parallel metadata operation, or document each intentional divergence.
- L-0006 (1×, #1641): Validate server-controlled metadata column count and names before positional access, or resolve by name, to detect schema drift.
- L-0007 (1×, #1672): Explicit connection flags win; otherwise a default-enabled client flag defers to the server rollout flag, while a default-disabled feature stays off.
- L-0008 (2×, #1672 #1747): Document connection-property behavior and opt-out without exposing internal rollout flags. Removing a property also requires updating raw-key tests and documenting ignored legacy settings.
- L-0009 (1×, #1680): Preserve JDBC-required metadata ordering, including getTypeInfo ordering by DATA_TYPE, and test ordering separately from column contents.
- L-0010 (1×, #1683): Use Locale.ROOT when normalizing identifiers, hostnames and cache keys.
- L-0011 (1×, #1656): Add NEXT_CHANGELOG.md for user-visible changes, including dependency bumps; CI/tooling and documentation may use NO_CHANGELOG=true.
- L-0012 (3×, #1707): Share OAuth configuration parsing across U2M and M2M flows, treat blank overrides deliberately, and verify SDK forwarding in actual outbound requests.
- L-0013 (1×, #1713): Keep related accessors consistent on missing/null/invalid keys; guard null before Guava getIfPresent when the API promises an empty result.
- L-0014 (1×, #1728): Case-insensitive JDBC label lookup is first-wins. Use putIfAbsent after normalization so duplicate labels work consistently across SEA and Thrift.
- L-0015 (1×, #1746): Carry SEA routing state through SessionInfo, SessionId serialization/equality and StatementId; extend serialized IDs with optional trailing parts and test old/new round trips.
- L-0016 (1×, #1739): Compute JDBC long-position/int-count windows in long, clamp before casting, and test Integer.MAX_VALUE counts and positions beyond int range.
- L-0017 (1×, #1739): Strip type parameters before enum lookup, derive DECIMAL precision/scale from its declaration, and map TIMESTAMP_NTZ to JDBC TIMESTAMP.
- L-0018 (1×, #1797): ResultSet configuration must reach session-less metadata, Array and in-memory results, or its documented scope must explicitly cover query results only.
- L-0019 (1×, #1797): Call checkIfClosed before argument validation or early return in JDBC getters; closed objects must consistently report RESULT_SET_CLOSED.

## Results and complex types
- L-0020 (2×, #1659 #1700): Handle null ARRAY/MAP/STRUCT elements before type-specific dispatch, for every element type.
- L-0021 (1×, #1700): Split complex-type metadata with depth- and quote-aware parsing; consumers must use structured parts instead of splitting flattened text again.
- L-0022 (1×, #1700): Defensively copy mutable arrays and collections on both input and output.
- L-0023 (1×, #1700): Share formatting across top-level, nested and feature-flag paths, including special cases such as omitted zero components.
- L-0024 (1×, #1700): When transforming one nested value type, preserve every other field's serialization and JSON-escape string leaves; domain toString is not container serialization.
- L-0025 (1×, #1731): SEA manifests may omit total_row_count. Continue via next_chunk_index and provider exhaustion; use an explicit unknown-count sentinel before row-count arithmetic.
- L-0026 (1×, #1731): Enumerate CloudFetch/SEA/Arrow flag combinations when changing provider routing; an unrelated false flag can accidentally select a downstream branch.
- L-0027 (1×, log:2026-10-22): Reproduce response-format bugs on the cluster/protocol that emits that format; a warehouse test cannot establish behavior of an APC-only response.
- L-0028 (2×, #1734): Select SEA result providers from actual response shape. URL_BASED_SET may contain inline Arrow with null links, including inline-plus-continuation responses.
- L-0029 (1×, log:2026-10-22): Parameterize complex-type E2E tests across SEA and Thrift with EnableComplexDatatypeSupport=1.
- L-0030 (1×, #1734): Advance the chunk provider successfully before updating ResultSet cursor state; failed advancement returns false without reading a nonexistent chunk.
- L-0031 (1×, log:2026-10-27): Use explicit catalog/schema in live metadata tests to avoid warehouse-wide scans; cover null-catalog behavior in unit tests.
- L-0032 (2×, #1623 #1625): Separate pre-submission, execution and post-commit parsing failures. Never report committed batch work as EXECUTE_FAILED, and clear retryable stored batch state before propagating errors.
- L-0033 (1×, #1623): Exclude multi-row VALUES INSERTs from native batching or rewrite to the backend's single-row form; unsupported shapes stay on the legacy path.
- L-0034 (1×, #1781): Calendar-cutover tests must cover every date from 1582-10-05 through 1582-10-14 and account for the gap plus a timezone margin.
- L-0035 (1×, #1781): Nested Arrow TIMESTAMP_NTZ arrays may have 5–7 components; default omitted seconds/nanos to zero and test exact-minute values.
- L-0036 (1×, #1781): Share conversion lookup, zone handling and errors across ResultSet and nested Array/Struct accessors; compare nested/top-level behavior under a non-UTC JVM zone.
- L-0037 (1×, #1784): Shield Arrow-wrapped live HTTP streams from interrupt-driven channel close; let the reading thread close them and abort failed responses instead of draining.
- L-0038 (1×, #1784): After Arrow reads, check the interrupt flag before success. SocketTimeoutException remains retryable; purge partial buffers on IO/runtime failure and assert zero allocated memory.
- L-0039 (1×, #1632): SEA polling should send a lightweight statement-id request; reusing a large execute request can resend its payload on every poll.
- L-0040 (1×, #1813): SQL classification must handle WITH-prefixed DML and Spark lexical rules, including backslash escapes, raw strings and continued line comments.

## Errors and telemetry
- L-0041 (4×, #1629 #1739 #1752): Use registered DatabricksDriverErrorCode values and the SQLException constructor that preserves numeric vendor code. Assert SQLSTATE/name/code with a distinguishable cause and record telemetry classification.
- L-0042 (1×, #1670): Compare JUL levels by intValue; equivalent levels need not share object identity.
- L-0043 (2×, #1683): Preserve the original error across recovery failures, including unchecked failures; inspect getSuppressed when suppression holds the original exception.
- L-0044 (1×, #1709): UserAgent.sanitize performs replacement without IllegalArgumentException; withOtherInfo key validation is the throwing operation.
- L-0045 (1×, #1762): Log statement IDs through loggableStatementId and select status fields lazily; raw generated Thrift toString output can expose secrets.
- L-0046 (1×, #1762): Capture fully formatted JUL messages with SimpleFormatter.formatMessage so negative secret-logging tests cannot pass on unexpanded parameters.
- L-0047 (1×, #1762): JdbcLogger uses {} placeholders under both JUL and SLF4J; avoid printf placeholders.
- L-0048 (1×, #1625): Register each new DatabricksMetricsTimed client method in TelemetryHelper.mapMethodToOperationType and test the mapping.

## Concurrency and caching
- L-0049 (1×, #1670): Mark singleton initialization complete only after useful configuration is installed; disabled, failed or reentrant attempts must remain retryable.
- L-0050 (2×, #1683 #1778 #1776): Bound TTL caches by size and freshness, use an injectable monotonic clock, and test expiry boundaries. Avoid network single-flight across differing connection HTTP configurations.
- L-0051 (2×, #1683 #1783): Best-effort caches must fall back safely on runtime failures and preserve uncached freshness guarantees. Clear shared caches around tests; warn when recovery overrides explicit user configuration.
- L-0052 (1×, #1678): Choose one synchronization protocol per field; avoid redundant atomics inside monitor-protected mutation paths.
- L-0053 (2×, #1709 #1726): SDK UserAgent registration is process-global and append-only. Register each distinct entry once, bound growth, and restore global state in tests.
- L-0054 (1×, #1726): SEA and Thrift obtain User-Agent through different paths; verify both transports after changing shared registration.
- L-0055 (1×, #1758): Apply connection-pool settings to both SEA and Thrift HTTP clients; test enabled and disabled/zero behavior through observed connection reuse.
- L-0056 (1×, #1769): Leak tests must inspect their own resource handles and clean only their singleton entries, avoiding global thread counts and global resets.
- L-0057 (1×, #1772 #1776): Keep network, auth, close and shutdown work outside ConcurrentHashMap compute lambdas; test unrelated colliding keys to expose lock contention.
- L-0058 (1×, #1776): Lazy-init holders must preserve computeIfAbsent failure/retry semantics. Remove failed holders conditionally and test concurrent removal and same-thread reentry.
- L-0059 (1×, #1772): Cancel timed-out or interrupted futures, restore the interrupt flag, and document queue time and separate sequential wait budgets.
- L-0060 (1×, #1778): Shared HTTP-response cache keys must include every TLS trust input. Cache only valid 200 responses without credentials or cross-origin/path redirects; vary each key input in tests.

## Verification and dependencies
- L-0061 (1×, #1582): Required workflows must handle merge_group. Skip jobs requiring pull_request fields in queue events while retaining their PR-time gate.
- L-0062 (2×, #1663 #1751): Upgrade coupled HTTP libraries as a published-compatible set and exercise HTTP paths. Inspect shaded JAR namespaces, adding relocations and NOTICE entries for newly bundled dependencies.
- L-0063 (2×, #1641 #1700): Exercise the real caller path and compare against an independent reference; wrappers delegating to the same helper do not prove equivalence.
- L-0064 (2×, #1703 #1754): Update all SEA WireMock paths and exact request bodies when wire behavior changes, removing obsolete mappings.
- L-0065 (1×, #1723): Avoid HTTP gzip over already-compressed LZ4 Arrow by default; use identity encoding and make recompression an explicit bandwidth tradeoff.
- L-0066 (1×, #1700): Decode real producer wire fixtures; round trips through the same writer can hide production serialization quirks.
- L-0067 (1×, #1753): Before offline Maven tests, warm Surefire providers and matching junit-platform-launcher, normalize repository IDs, and keep bot warm-up checks best-effort.
- L-0068 (2×, #1790 #1808 #1783): Performance regressions must fail on revert. Use warmed hot-path call assertions and differential tests against the old iterator/state machine, including random inputs and call orders.
- L-0069 (1×, #1633): Ordering tests need ignoreArrayOrder=false and distinguishable per-element values; identical elements or update counts cannot detect reordering.

## Bot workflow
- L-0070 (2×, log:2026-08-14): Keep diagnosis/plan phases read-only and carry precise findings into implementation prompts to avoid repeated discovery.
- L-0071 (3×, #1582 #1739 #1742): State agreed environment restrictions once. Stop repeated identical bot setup/commit failures, request human resolution, and label uncompiled or untested edits as unverified.
- L-0072 (1×, log:2026-10-22): Author-run environments lack WebFetch/WebSearch; diagnose from repository context and available tools.
