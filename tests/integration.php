<?php

/**
 * @package     Localzet Storage
 * @copyright   Copyright © 2016-2026 Localzet Group. All rights reserved.
 * @license     https://www.gnu.org/licenses/agpl-3.0 GNU Affero General Public License v3.0
 */

use localzet\Storage\Client;

require dirname(__DIR__) . '/vendor/autoload.php';

$probe = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
if ($probe === false) {
    throw new RuntimeException('Could not allocate a test port.');
}
$address = stream_socket_get_name($probe, false);
fclose($probe);
$port = (int)substr(strrchr($address, ':'), 1);
$serverScript = __DIR__ . '/storage_server.php';
$process = proc_open(
    [PHP_BINARY, '-d', 'disable_functions=pcntl_fork', $serverScript, 'start', (string)$port],
    [0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'a'], 2 => ['file', '/dev/null', 'a']],
    $pipes,
    dirname(__DIR__)
);
if (!is_resource($process)) {
    throw new RuntimeException('Could not start the storage test server.');
}

try {
    $deadline = microtime(true) + 8;
    $ready = false;
    while (microtime(true) < $deadline) {
        $socket = @fsockopen('127.0.0.1', $port, $errno, $error, 0.1);
        if ($socket !== false) {
            fclose($socket);
            $ready = true;
            break;
        }
        $status = proc_get_status($process);
        if (!$status['running']) {
            throw new RuntimeException('Storage server exited before accepting connections.');
        }
        usleep(50_000);
    }
    if (!$ready) {
        throw new RuntimeException('Storage server did not start in time.');
    }

    $client = new Client("127.0.0.1:$port");
    if ($client->set('integration-key', 'stored-value') !== true) {
        throw new RuntimeException('Storage set request failed.');
    }
    if ($client->get('integration-key') !== 'stored-value') {
        throw new RuntimeException('Storage get request returned an unexpected value.');
    }
    if ($client->add('integration-key', 'duplicate') !== false) {
        throw new RuntimeException('Storage add must reject an existing key.');
    }
    if ($client->delete('integration-key') !== true || $client->get('integration-key') !== null) {
        throw new RuntimeException('Storage delete request failed.');
    }

    echo "Storage loopback integration checks passed.\n";
} finally {
    proc_terminate($process);
    proc_close($process);
}
