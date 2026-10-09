# Compatibility and verification

The package targets PHP 8.3+ and Laravel 11, 12, and 13. Redis Cluster and Horizon-managed fair workers are unsupported; see the [README](../README.md#limitations) for other limitations.

## Local verification

On local Windows/PHP 8.3, the full suite passed against Laravel 11.57.0, 12.69.3, and 13.35.0 (19 tests and 1,169 assertions per version). Composer validation and Pint passed; PHPStan level 6 passed against Laravel 13.

Linux, PHP 8.4, deployment failover, and sustained load remain CI and operational release gates. These local results alone are not production certification.

## Laravel 11 dependency advisories

Composer audit reported four advisories against the Laravel 11 framework used in the compatibility run. Applications choosing Laravel 11 must evaluate those upstream advisories and their own exposure. Prefer a patched Laravel 12 or 13 release for new deployments, and run `composer audit` against the host application's resolved dependency set.

The CI matrix keeps Laravel 11 compatibility coverage by passing Composer's `--no-security-blocking` option only while resolving that matrix job. Laravel 12 and 13 jobs retain normal blocking. This exception permits a test dependency to install; it does not resolve or dismiss the Laravel 11 advisories.
