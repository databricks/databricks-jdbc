package com.databricks.jdbc.integration.fakeservice.tests;

import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;

/**
 * SQL Exec API integration tests. The original JSON inline chunked results test
 * (testJsonInlineChunkedResults_withoutArrow) was removed because EnableArrow=0 is now deprecated
 * and ignored — Arrow is always enabled on non-AIX platforms. The WireMock recordings were captured
 * with JSON_ARRAY format which no longer matches the driver's requests. Chunked Arrow results are
 * covered by MultiChunkExecutionIntegrationTests and SqlExecApiHybridResultsIntegrationTests.
 */
public class SqlExecApiIntegrationTests extends AbstractFakeServiceIntegrationTests {}
