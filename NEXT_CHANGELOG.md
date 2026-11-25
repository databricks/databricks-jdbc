# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed

- Fixed revoked server certificates being accepted when `CheckCertRevocation=1` or `AcceptUndeterminedRevocation=0` was set and the connection used the default Java trust store (no `SSLTrustStore`, `UseSystemTrustStore` or `AllowSelfSignedCerts`). These connections now check revocation for queries (Thrift and SEA), CloudFetch downloads, telemetry and authentication. If the driver cannot reach the certificate's OCSP or CRL server, the connection now fails. This includes networks where outbound traffic only goes through `UseProxy`, because Java's revocation checks do not use the driver's proxy. To keep these connections working, set `AcceptUndeterminedRevocation=1`, allow direct access to the revocation servers, or set the JVM `http.proxyHost` property. Connections that set neither property, or that set `AcceptUndeterminedRevocation=1`, behave as before. Asynchronous volume uploads still do not check revocation.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
