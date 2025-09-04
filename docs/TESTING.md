# Testing Documentation

## Integration Tests

The project includes integration tests located in `src/test/java/com/databricks/jdbc/integration/fakeservice/tests`.
These tests run against fake services that mimic production services like `SQL_EXEC`, `SQL_GATEWAY`, and `THRIFT_SERVER`.

### Fake Service Modes

The fake service can operate in three modes, controlled by the environment variable `FAKE_SERVICE_TEST_MODE`:

1. **RECORD**: Records responses from production services and saves them to `/src/test/resources/`
2. **REPLAY** (default): Replays recorded responses without connecting to production services
3. **DRY**: Connects to production services with fake service acting as a pass-through proxy

### Fake Service Types

The fake service can emulate different Databricks service types, controlled by the environment variable `FAKE_SERVICE_TYPE`:

1. **SQL_EXEC** (default): Emulates the SQL Execution service (SQL warehouse compute)
2. **SQL_GATEWAY**: Emulates the SQL Gateway service (SQL warehouse compute)
3. **THRIFT_SERVER**: Emulates the Thrift Server service (All-purpose compute)

The appropriate client type will be automatically selected based on the `FAKE_SERVICE_TYPE`.

### Authentication Token

For RECORD and DRY modes, which connect to actual Databricks services, authentication is required via the environment variable `DATABRICKS_TOKEN`:

- **DATABRICKS_TOKEN**: A valid Databricks Personal Access Token (PAT) for the environment specified in the properties files
- The token is not required for REPLAY mode, which uses pre-recorded responses
- For security, always use environment variables rather than hardcoding tokens in test files
- The token should have permissions to access the SQL warehouses or clusters defined in the properties files

### Properties Files

Each fake service type has its own properties file in the `src/test/resources/` directory:

1. **SQL_EXEC**: Uses `sqlexecfakeservicetest.properties`
2. **SQL_GATEWAY**: Uses `sqlgatewayfakeservicetest.properties`
3. **THRIFT_SERVER**: Uses `thriftserverfakeservicetest.properties`

The `FAKE_SERVICE_TYPE` environment variable determines which properties file is loaded. These files contain configuration values required for testing, including:

- `host.databricks`: The Databricks host URL
- `host.cloudfetch`: The cloud storage URL for fetching results
- `httppath`: The HTTP path for the compute resource
    - SQL warehouse paths (for SQL_EXEC and SQL_GATEWAY) use format: `/sql/1.0/warehouses/<warehouse-id>`
    - All-purpose compute paths (for THRIFT_SERVER) use format: `/sql/protocolv1/o/<org-id>/<cluster-id>`
- `connschema`: The default schema to connect to
- `conncatalog`: The default catalog to connect to
- `testcatalog`: The catalog to use for creating test tables
- `testschema`: The schema to use for creating test tables

### Running Integration Tests

#### Examples

Run connection tests using SQL_GATEWAY in REPLAY mode:
```bash
FAKE_SERVICE_TYPE=SQL_GATEWAY FAKE_SERVICE_TEST_MODE=replay mvn -Dtest=com.databricks.jdbc.integration.fakeservice.tests.ConnectionIntegrationTests test
```

Run all tests using THRIFT_SERVER in REPLAY mode:
```bash
FAKE_SERVICE_TYPE=THRIFT_SERVER FAKE_SERVICE_TEST_MODE=replay mvn -Dtest=*IntegrationTests test
```

Run execution tests using the default SQL_EXEC in RECORD mode:
```bash
DATABRICKS_TOKEN=<personal-access-token> FAKE_SERVICE_TEST_MODE=record mvn -Dtest=com.databricks.jdbc.integration.fakeservice.tests.ExecutionIntegrationTests test
```

For RECORD or DRY modes, you need to set a personal access token:
```bash
DATABRICKS_TOKEN=<personal-access-token> FAKE_SERVICE_TYPE=SQL_GATEWAY FAKE_SERVICE_TEST_MODE=record mvn -Dtest=*IntegrationTests test
```

## Test Class Naming Conventions

- Classes ending with `Test` are unit tests
- Classes under `com/databricks/jdbc/integration/e2e` are the highest fidelity end-to-end tests
- Classes under `com/databricks/jdbc/integration/fakeservice/tests` are fake service tests
