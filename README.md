# Databricks JDBC Driver

The Databricks JDBC driver implements the JDBC interface providing connectivity to a Databricks SQL warehouse.
Please refer to [Databricks documentation](https://docs.databricks.com/aws/en/integrations/jdbc-oss/) for more
information.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Prerequisites

Databricks JDBC is compatible with Java 11 and higher. CI testing runs on Java versions 11, 17, and 21.

## Installation

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
  <groupId>com.databricks</groupId>
  <artifactId>databricks-jdbc</artifactId>
  <version>3.0.5</version>
</dependency>
```

### Build from Source

1. Clone the repository
2. Run the following command:
   ```bash
   mvn clean package
   ```
3. The jar file is generated as `target/databricks-jdbc-<version>.jar`
4. The test coverage report is generated in `target/site/jacoco/index.html`

## Usage

### Connection String

```
jdbc:databricks://<host>:<port>;transportMode=http;ssl=1;AuthMech=3;httpPath=<path>;UID=token;PWD=<token>
```

### Authentication

The JDBC driver supports the following authentication methods:

#### Personal Access Token (PAT)

Use `AuthMech=3` for personal access token authentication:

```
AuthMech=3;UID=token;PWD=<your_token>
```

#### OAuth2 Authentication

Use `AuthMech=11` for OAuth2-based authentication. Several OAuth flows are supported:

##### Token Passthrough

Direct use of an existing OAuth token:

```
AuthMech=11;Auth_Flow=0;Auth_AccessToken=<your_access_token>
```

##### OAuth Client Credentials (Machine-to-Machine)

Configure standard OAuth client credentials flow:

```
AuthMech=11;Auth_Flow=1;OAuth2ClientId=<client_id>;OAuth2Secret=<client_secret>
```

Optional parameters:
- `AzureTenantId`: Azure tenant ID for Azure Databricks (default: null). If enabled, the driver will include refreshed
Azure Active Directory (AAD) Service Principal OAuth tokens with every request.

##### Browser-Based OAuth

Interactive browser-based OAuth flow with PKCE:

```
AuthMech=11;Auth_Flow=2
```

Optional parameters:
- `OAuth2ClientId` - Client ID for OAuth2 (default: databricks-cli)
- `OAuth2RedirectUrlPort` - Ports for redirect URL (default: 8020)
- `EnableOIDCDiscovery` - Enable OIDC discovery (default: 1)
- `OAuthDiscoveryURL` - OIDC discovery endpoint (default: /oidc/.well-known/oauth-authorization-server)
- `EnableSQLValidationForIsValid` - Enable SQL query based validation in `isValid()` connection checks (default: 0)

### Logging

The driver supports both SLF4J and Java Util Logging (JUL) frameworks:

- **SLF4J**: Enable with `-Dcom.databricks.jdbc.loggerImpl=SLF4JLOGGER`
- **JUL**: Enable with `-Dcom.databricks.jdbc.loggerImpl=JDKLOGGER` (default)

For detailed logging configuration options, see [Logging Documentation](./docs/LOGGING.md).

## Running Tests

Basic test execution:

```bash
mvn test
```

**Note**: Due to a change in JDK 16 that introduced a compatibility issue with the Apache Arrow library used by the JDBC
driver, runtime errors may occur when using the JDBC driver with JDK 16 or later. To avoid these errors, restart your
application or driver with the following JVM command option:

```
--add-opens=java.base/java.nio=org.apache.arrow.memory.core ALL-UNNAMED
```

For more detailed information about integration tests and fake services, see [Testing Documentation](./docs/TESTING.md).

## Documentation

For more information, see the following resources:
- [Integration Tests Guide](./docs/TESTING.md)
- [Logging Configuration](./docs/LOGGING.md)
