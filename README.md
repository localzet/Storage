# Localzet Storage

[Русская документация](README.ru.md)

A client/server shared in-memory data store for Localzet Server applications.

## Status and compatibility

The transport uses PHP serialization. Keep its ports on a trusted network; untrusted input, object deserialization, frame bounds, concurrent operations and recovery require review before deployment.

The component targets Server 7.x. The server binds to loopback by default and has no client authentication; use it only on a trusted network and explicitly configure any non-loopback bind behind network access controls. PHP object deserialization is disabled by default when reading network data. For a trusted, isolated network, enable it explicitly with `new Server($ip, $port, allowObjectSerialization: true)` and `new Client($servers, allowObjectSerialization: true)`. Do not enable this mode for untrusted clients.

## Dependencies

- `php`: `^8.2`
- `localzet/server`: `^7.0`

## Installation

```sh
composer config repositories.localzet-storage vcs https://github.com/localzet/Storage
composer require localzet/storage:dev-main
```

The package is not published on Packagist yet; this VCS source installs the current Server 7 branch.

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
