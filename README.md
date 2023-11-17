# databricks-jdbc
Repository for Java connector for Databricks

**Status**: In Development

The Databricks JDBC driver implements the JDBC interface providing connectivity to a Databricks SQL warehouse

## Getting started
You can install Databricks JDBC driver by adding the following to your `pom.xml`:

```pom.xml
<dependency>
  <groupId>com.databricks</groupId>
  <artifactId>databricks-jdbc-oss</artifactId>
  <version>0.0.1</version>
</dependency>
```

## Instructions for building
From development or main branch, run `mvn clean package`

The jar file is generated as target/databricks-jdbc-oss-jar-with-dependencies.jar

## Authentication
The JDBC driver supports following modes for authentication:

1. Personal Access Tokens: Set AuthMech=3 in connection string to use Personal Access Tokens, which can be set using PWD property.
2. OAuth2: Set AuthMech=11 for using OAuth2. We only support Azure and AWS as cloud providers for OAuth2.
   - Access Token: Set Auth_Flow=0 for providing passthrough access token using PWD property.
   - Client Credentials: Set Auth_Flow=1 for using Machine-to-machine OAuth flow.
   - Browser based OAuth: Set Auth_Flow=2 for using User-to-machine OAuth flow.


