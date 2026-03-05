---
description: Debug and fix a GitHub issue end-to-end — reproduce, fix, test, create PR, and research related issues.
---

### User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding. The input should be a GitHub issue number (e.g., `1247`) or a full GitHub issue URL. If empty, ask the user for an issue number.

## Goal

Fix a GitHub issue with a minimal, well-tested change and create a PR linked to the issue. After the fix, research the codebase for related patterns that may have the same bug.

## Steps

### Step 1: Fetch and Understand the Issue

1. Determine the target repo from the current git remote (default to `databricks/databricks-jdbc`) and use it as the resolved repo for all subsequent `gh` commands.
2. Use `gh issue view <number> --repo <resolved-repo>` to fetch the issue title, description, reproduction steps, expected vs actual behavior, and environment details.
3. Summarize your understanding of the bug to the user and ask for confirmation before proceeding.

### Step 2: Reproduce the Issue

Write a **minimal failing test** that demonstrates the bug:

1. Identify the relevant module and test directory.
2. Write a unit test that reproduces the exact scenario described in the issue. Prefer unit tests over integration tests when possible.
3. Run the test and **confirm it fails** with the expected error. Show the failure output to the user.
4. If the bug requires E2E/integration testing against a live workspace:
   - Verify the required environment variables are set: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and `DATABRICKS_HTTP_PATH`.
   - If any are missing, ask the user to set them before proceeding.
   - Ask if they want to proceed with integration tests or stick with unit tests.

### Step 3: Identify Root Cause

1. Use the Explore agent or search tools to trace the code path involved in the bug.
2. Identify the **minimal set of files and methods** that need to change.
3. Explain the root cause to the user with file paths and line numbers.
4. Confirm the fix approach before making changes — keep the blast radius minimal.

### Step 4: Implement the Fix

1. Make the **smallest code change** that fixes the bug. No unrelated refactors, no over-engineering.
2. Follow existing code patterns and conventions.
3. Run the failing test from Step 2 and confirm it now **passes**.
4. Run the full test class to check for regressions.

### Step 5: Add Tests

Ensure adequate test coverage for the fix. **Add tests to existing test suites whenever possible — only create a new test file if absolutely necessary.**

1. The reproduction test from Step 2 should already cover the primary case.
2. Add edge case tests as needed (e.g., null values, boundary conditions, alternate input formats). Prefer parameterised tests (e.g., JUnit `@ParameterizedTest`) wherever possible to cover multiple inputs concisely.
3. Add tests for related code paths if the same bug pattern applies (e.g., if a bug in STRUCT also affects ARRAY and MAP, test all three).
4. Run the full test suite for affected test classes and confirm all pass.

### Step 6: Create PR

1. Ensure the correct GitHub account is active for this repository, following `CLAUDE.md` guidance. Check with `gh auth status` and switch if needed with `gh auth switch --user <account>`.
2. Create a descriptive branch: `fix/<issue-number>-<short-description>`
3. Commit with DCO sign-off (`-s` flag) and a clear message referencing the issue.
4. Push and create a PR with:
   - Title referencing the issue
   - Summary section explaining the bug and fix
   - Test plan section listing all tests added
   - `Closes #<issue-number>` in the body
   - A `NEXT_CHANGELOG.md` entry for user-facing changes, or `NO_CHANGELOG=true` for internal-only changes (see `CLAUDE.md` for guidance; ask the user if unclear)
5. Share the PR URL with the user.

### Step 7: Research Related Issues

**After the fix is complete**, proactively research the codebase for similar patterns:

1. Search for code that follows the same pattern as the bug (e.g., if a type conversion was wrong for DATE, check if TIMESTAMP, TIME, etc. have the same issue).
2. Search for similar method calls, switch cases, or conversion logic that could be affected.
3. Check if the same fix pattern needs to be applied elsewhere (e.g., in `DatabricksArray`, `DatabricksStruct`, or other parsers).
4. Report findings to the user as a list of potential related issues with file paths and line numbers.
5. Let the user decide whether to fix those in the same PR or create follow-up issues.

## Important Notes

- Always read files before modifying them.
- Follow the project's coding conventions (Google Java Style Guide, run `mvn spotless:apply`).
- Keep changes minimal — fix only what's broken.
- The reproduction test must fail before the fix and pass after.
- Research related issues is not optional — always do it as the final step.
