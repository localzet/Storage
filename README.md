# Localzet Storage

[Русская документация](README.ru.md)

A client/server shared in-memory data store for Localzet Server applications.

## Status and compatibility

Current release: `2.0.0`. The package targets Localzet Server 7 and fixes client framing for partial header/body reads and writes. Version 2 changes the default payload contract: object values and recursive arrays are rejected, while scalars and acyclic arrays remain supported. Version 1 clients that send PHP objects must migrate before upgrading.

The transport uses PHP serialization inside length-prefixed frames (maximum payload: 64 MiB). Keep its ports on a trusted network. The store is in-memory and process-local: it has no persistence, replication, client authentication or concurrency control.

The component targets Server 7.x. The server binds to loopback by default and has no client authentication; use it only on a trusted network and explicitly configure any non-loopback bind behind network access controls. When object support is disabled, the server and client reject PHP object values, including `__PHP_Incomplete_Class`, and recursive arrays. For a trusted, isolated network, enable object serialization explicitly on both peers with `new Server($ip, $port, allowObjectSerialization: true)` and `new Client($servers, allowObjectSerialization: true)`. Do not enable this mode for untrusted clients.

### Migration from 1.x

Before upgrading, remove PHP objects and recursive arrays from stored values. If those values must remain, configure the object serialization opt-in on both server and client and keep the connection on a trusted, isolated network. Version 1.x framing is not compatible with Localzet Server 4; the 1.x release line already targets Server 7.

## Dependencies

- `php`: `^8.2`
- `localzet/server`: `^7.0`

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
