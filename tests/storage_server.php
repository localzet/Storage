<?php

/**
 * @package     Localzet Storage
 * @copyright   Copyright © 2016-2026 Localzet Group. All rights reserved.
 * @license     https://www.gnu.org/licenses/agpl-3.0 GNU Affero General Public License v3.0
 */

require dirname(__DIR__) . '/vendor/autoload.php';

$port = (int)($argv[2] ?? 0);
if ($port < 1 || $port > 65535) {
    throw new InvalidArgumentException('A valid test port is required.');
}

\localzet\Server::$pidFile = sys_get_temp_dir() . '/localzet-storage-test-' . getmypid() . '.pid';
\localzet\Server::$logFile = sys_get_temp_dir() . '/localzet-storage-test-' . getmypid() . '.log';

new \localzet\Storage\Server('127.0.0.1', $port);
\localzet\Server::runAll();
