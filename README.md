# Databricks JDBC Driver

The Databricks JDBC driver implements the JDBC interface providing connectivity to a Databricks SQL warehouse.

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
  <version>1.0.4-oss</version>
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

##### OAuth with JWT Assertion (Private Key Authentication)

For JWT-based authentication using a private key:

```
AuthMech=11;Auth_Flow=1;UseJWTAssertion=1;OAuth2ClientId=<client_id>;Auth_JWT_Key_File=<path_to_key>;Auth_KID=<key_id>;Auth_JWT_Alg=RS256
```

Optional parameters:
- `Auth_JWT_Key_Passphrase` - If your key file is password-protected
- `Auth_JWT_Alg` - Supported algorithms: RS256 (default), RS384, RS512, PS256, PS384, PS512, ES256, ES384, ES512

##### OAuth with Refresh Token

For OAuth using a refresh token to obtain new access tokens:

```
AuthMech=11;Auth_Flow=1;OAuth2ClientId=<client_id>;OAuth2Secret=<client_secret>;OAuthRefreshToken=<refresh_token>
```

##### Browser-Based OAuth

Interactive browser-based OAuth flow:

```
AuthMech=11;Auth_Flow=2;OAuth2ClientId=<client_id>;OAuthDiscoveryURL=<discovery_url>
```

Optional parameters:
- `OAuth2RedirectUrlPort` - Port for redirect URL (default: 8020)
- `EnableOIDCDiscovery` - Enable OIDC discovery (default: 1)
- `Auth_Scope` - OAuth scope (default: all-apis)

#### Azure Managed Service Identity (MSI)

For Azure environments, use Azure Managed Service Identity:

```
AuthMech=11;Auth_Flow=3
```

Optional parameters:
- `azure_workspace_resource_id` - Resource ID of the Azure Databricks workspace
- `GoogleServiceAccount` - For GCP service account authentication

### Logging

The driver supports both SLF4J and Java Util Logging (JUL) frameworks:

- **SLF4J**: Enable with `-Dcom.databricks.jdbc.loggerImpl=SLF4JLOGGER`
- **JUL**: Enable with `-Dcom.databricks.jdbc.loggerImpl=JDKLOGGER` (default)

For detailed logging configuration options, see [Logging Documentation](./docs/logging.md).

## Running Tests

Basic test execution:

```bash
mvn test
```

For more detailed information about integration tests and fake services, see [Testing Documentation](./docs/testing.md).

## Documentation

For more information, see the following resources:
- [Integration Tests Guide](./docs/testing.md)
- [Logging Configuration](./docs/logging.md)
