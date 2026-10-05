# Localzet Storage

[English documentation](README.md)

Клиент-серверное общее хранилище данных для приложений Localzet Server.

## Состояние и совместимость

Транспорт использует PHP serialization. Порты должны оставаться в доверенной сети; недоверенные данные, десериализация объектов, границы фреймов, конкурентные операции и восстановление требуют ревизии перед эксплуатацией.

Это компонент для Server 4.x; совместимость с Server 7.x не установлена.

## Зависимости

- `php`: `^8.2`
- `localzet/server`: `^4.1`

## Установка

```sh
composer require localzet/storage
```

## Проверки разработки

```sh
composer validate --strict
composer install
composer dump-autoload --optimize --strict-psr
composer audit
```

Установка, lint и автозагрузка не подтверждают сквозное поведение или готовность к эксплуатации.

## Автор и лицензия

Ivan Zorin (`localzet`), <creator@localzet.com>, https://www.localzet.com.
Source: https://github.com/localzet/Storage. AGPL-3.0-or-later; [LICENSE](LICENSE). Сохраняются исходные уведомления авторов и лицензии сторонних компонентов.

[Authors](.github/AUTHORS.md) · [Contributing](.github/CONTRIBUTING.md) · [Security](.github/SECURITY.md)
