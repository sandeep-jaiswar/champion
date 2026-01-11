# Security Considerations

## Known Vulnerabilities

### MLflow - Unsafe Deserialization (CVE pending)

**Status:** No patch available  
**Affected versions:** >= 0.5.0, <= 3.4.0  
**Current version:** 2.9.0+  
**Severity:** High

#### Description

MLflow versions up to 3.4.0 contain an unsafe deserialization vulnerability. Currently, there is no patched version available from the MLflow project.

#### Mitigation Strategies

Until a patch is available, the following mitigations should be implemented:

1. **Network Isolation**: Run MLflow tracking server in an isolated network segment
2. **Access Control**: Restrict access to MLflow UI and API to trusted users only
3. **Input Validation**: Do not load untrusted model artifacts or run files
4. **Monitoring**: Monitor MLflow service logs for suspicious activity
5. **Sandboxing**: Consider running MLflow in containerized environments with limited privileges

#### Recommendations

- Monitor MLflow security advisories: <https://github.com/mlflow/mlflow/security>
- Upgrade to patched version immediately when available
- Consider alternative experiment tracking solutions if security requirements are strict

## Resolved Vulnerabilities

### Prefect - CORS Misconfiguration

**Status:** Fixed  
**Affected versions:** < 2.20.17, >= 3.0.0rc1 < 3.0.3  
**Fixed version:** 2.20.17+, 3.0.3+  
**Resolution:** Upgraded to Prefect ^2.20.17

## Reporting Security Issues

If you discover a security vulnerability in this project, please report it to the maintainers privately before public disclosure.
