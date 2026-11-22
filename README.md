# Localzet Storage

[Русская документация](README.ru.md)

A client/server shared in-memory data store for Localzet Server applications.

## Status and compatibility

Current release: `2.0.2`. It requires Localzet Server 7.2.3 or newer within the 7.x series. Version 2.0.2 configures per-connection receive and send limits for the maximum supported 64 MiB payload, including its 4-byte frame header. Version 2 rejects object values and recursive arrays by default; version 2.0.1 added bounded payload validation before decoding. Version 1 clients that send PHP objects must migrate before upgrading.

The transport uses PHP serialization inside length-prefixed frames (maximum payload: 64 MiB, plus a 4-byte header). Storage applies the complete-frame limit to each connection's receive and send buffers without changing Server's process-wide defaults. Budget memory for concurrent large frames and keep the listener on a trusted network. In the default safe mode, validation rejects values with more than 100,000 array entries across the whole value tree or more than 128 nested array levels. The store is in-memory and process-local: it has no persistence, replication, client authentication or concurrency control.

The component requires Server 7.2.3 or newer within the 7.x line. The server binds to loopback by default and has no client authentication; use it only on a trusted network and explicitly configure any non-loopback bind behind network access controls. When object support is disabled, the server and client reject PHP object values, including `__PHP_Incomplete_Class`, recursive arrays, arrays with over 100,000 total values and arrays deeper than 128 levels. For a trusted, isolated network, enable object serialization explicitly on both peers with `new Server($ip, $port, allowObjectSerialization: true)` and `new Client($servers, allowObjectSerialization: true)`. This opt-in restores legacy PHP serialization behavior and skips these validation limits; never enable it for untrusted clients.

### Migration from 1.x

Before upgrading, remove PHP objects and recursive arrays from stored values. If those values must remain, configure the object serialization opt-in on both server and client and keep the connection on a trusted, isolated network. Version 1.x framing is not compatible with Localzet Server 4; the 1.x release line already targets Server 7.

## Dependencies

- `php`: `^8.2`
- `localzet/server`: `^7.2.3`

## Installation

```sh
composer config repositories.localzet-storage vcs https://github.com/localzet/Storage
composer require localzet/storage:^2.0
```

The package is not published on Packagist yet; Composer resolves the stable `2.x` tags from this VCS source.

## Quick start

Run the storage server as a separate CLI process:

```php
<?php

use localzet\Server;
use localzet\Storage\Server as StorageServer;

new StorageServer('127.0.0.1', 8008);
Server::runAll();
```

Connect from an application process:

```php
<?php

use localzet\Storage\Client;

$storage = new Client('127.0.0.1:8008');
$storage->set('greeting', 'hello');
$greeting = $storage->get('greeting');
```

Both sides must use the same compatible Server 7 framing. By default, PHP serialized payloads contain only scalars and acyclic arrays; only enable the constructor opt-in when every peer is trusted.

## Development checks

```sh
composer validate --strict
composer install
composer dump-autoload --optimize --strict-psr
composer run check
composer audit
```

Installation, lint and autoload checks do not establish end-to-end behavior or production readiness.

## Author and license

Ivan Zorin (`localzet`), <creator@localzet.com>, https://www.localzet.com.
Source: https://github.com/localzet/Storage. AGPL-3.0-or-later; [LICENSE](LICENSE). Original copyright and third-party licenses remain applicable.

[Authors](.github/AUTHORS.md) · [Contributing](.github/CONTRIBUTING.md) · [Security](.github/SECURITY.md)
