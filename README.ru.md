# Localzet Storage

[English documentation](README.md)

Клиент-серверное общее хранилище данных для приложений Localzet Server.

## Состояние и совместимость

Транспорт использует PHP serialization с length-prefixed кадрами (payload до 64 МиБ). Порты должны оставаться в доверенной сети. Хранилище работает только в памяти одного процесса: в нём нет персистентности, репликации, аутентификации клиентов и контроля конкурентных операций.

Компонент рассчитан на Server 7.x. По умолчанию сервер слушает loopback; встроенной аутентификации клиентов нет. Используйте его только в доверенной сети, а для привязки к внешнему интерфейсу заранее настройте сетевой контроль доступа. Десериализация PHP-объектов по умолчанию отключена. Для доверенной изолированной сети включайте её явно через `new Server($ip, $port, allowObjectSerialization: true)` и `new Client($servers, allowObjectSerialization: true)`. Не включайте этот режим для недоверенных клиентов.

## Зависимости

- `php`: `^8.2`
- `localzet/server`: `^7.0`

## Установка

```sh
composer config repositories.localzet-storage vcs https://github.com/localzet/Storage
composer require localzet/storage:dev-main
```

Пакет пока не опубликован в Packagist; указанный VCS source устанавливает текущую ветку Server 7.

## Быстрый запуск

Запустите сервер хранилища в отдельном CLI-процессе:

```php
<?php

use localzet\Server;
use localzet\Storage\Server as StorageServer;

new StorageServer('127.0.0.1', 8008);
Server::runAll();
```

Подключитесь к нему из приложения:

```php
<?php

use localzet\Storage\Client;

$storage = new Client('127.0.0.1:8008');
$storage->set('greeting', 'hello');
$greeting = $storage->get('greeting');
```

Обе стороны должны использовать совместимую framing-модель Server 7. Десериализация PHP-объектов из сети по умолчанию отключена; включайте constructor opt-in, только если доверяете каждому узлу.

## Проверки разработки

```sh
composer validate --strict
composer install
composer dump-autoload --optimize --strict-psr
composer run check
composer audit
```

Установка, lint и автозагрузка не подтверждают сквозное поведение или готовность к эксплуатации.

## Автор и лицензия

Ivan Zorin (`localzet`), <creator@localzet.com>, https://www.localzet.com.
Source: https://github.com/localzet/Storage. AGPL-3.0-or-later; [LICENSE](LICENSE). Сохраняются исходные уведомления авторов и лицензии сторонних компонентов.

[Authors](.github/AUTHORS.md) · [Contributing](.github/CONTRIBUTING.md) · [Security](.github/SECURITY.md)
