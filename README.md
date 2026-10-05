# Localzet Storage

[Русская документация](README.ru.md)

A client/server shared in-memory data store for Localzet Server applications.

## Status and compatibility

The transport uses PHP serialization. Keep its ports on a trusted network; untrusted input, object deserialization, frame bounds, concurrent operations and recovery require review before deployment.

This is a Server 4.x component; Server 7.x compatibility is not established.

## Dependencies

- `php`: `^8.2`
- `localzet/server`: `^4.1`

## Installation

```sh
composer require localzet/storage
```

## Development checks

```sh
composer validate --strict
composer install
composer dump-autoload --optimize --strict-psr
composer audit
```

Installation, lint and autoload checks do not establish end-to-end behavior or production readiness.

## Author and license

Ivan Zorin (`localzet`), <creator@localzet.com>, https://www.localzet.com.
Source: https://github.com/localzet/Storage. AGPL-3.0-or-later; [LICENSE](LICENSE). Original copyright and third-party licenses remain applicable.

[Authors](.github/AUTHORS.md) · [Contributing](.github/CONTRIBUTING.md) · [Security](.github/SECURITY.md)
