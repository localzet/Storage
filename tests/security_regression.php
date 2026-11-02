<?php
/**
 * @package     Localzet Storage
 * @link        https://github.com/localzet/Storage
 *
 * @author      Ivan Zorin <creator@localzet.com>
 * @copyright   Copyright © 2016-2026 Localzet Group. All rights reserved.
 * @license     https://www.gnu.org/licenses/agpl-3.0 GNU Affero General Public License v3.0
 *
 *              This program is free software: you can redistribute it and/or modify
 *              it under the terms of the GNU Affero General Public License as published
 *              by the Free Software Foundation, either version 3 of the License, or
 *              (at your option) any later version.
 *
 *              This program is distributed in the hope that it will be useful,
 *              but WITHOUT ANY WARRANTY; without even the implied warranty of
 *              MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *              GNU Affero General Public License for more details.
 *
 *              You should have received a copy of the GNU Affero General Public License
 *              along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
 *              For any questions, please contact <creator@localzet.com>
*/

use localzet\Storage\Client;
use localzet\Storage\Server;

final class StorageWakeupProbe
{
    public static bool $wokeUp = false;

    public function __wakeup(): void
    {
        self::$wokeUp = true;
    }
}

require dirname(__DIR__) . '/vendor/autoload.php';

final class StorageClientProbe extends Client
{
    public function writeFrame(array $data, $connection): void
    {
        $this->write($data, $connection);
    }

    public function readFrame($connection): mixed
    {
        return $this->read($connection);
    }
}

$client = new Client('127.0.0.1:8008');
$clientProperty = new ReflectionProperty($client, 'allowObjectSerialization');
$clientProperty->setAccessible(true);
if ($clientProperty->getValue($client) !== false) {
    throw new RuntimeException('Client object deserialization must be disabled by default.');
}

$trustedClient = new Client('127.0.0.1:8008', allowObjectSerialization: true);
if ($clientProperty->getValue($trustedClient) !== true) {
    throw new RuntimeException('Client trusted-network opt-in was not retained.');
}

$server = new Server('127.0.0.1', 8008);
$serverProperty = new ReflectionProperty($server, 'allowObjectSerialization');
$serverProperty->setAccessible(true);
if ($serverProperty->getValue($server) !== false) {
    throw new RuntimeException('Server object deserialization must be disabled by default.');
}
$coreServerProperty = new ReflectionProperty($server, '_server');
$coreServerProperty->setAccessible(true);
if ($coreServerProperty->getValue($server)->getSocketName() !== 'frame://127.0.0.1:8008') {
    throw new RuntimeException('Storage server must bind to loopback by default.');
}
$connection = new class {
    public array $sent = [];
    public function send($data): bool { $this->sent[] = $data; return true; }
    public function close($data = ''): bool { $this->sent[] = $data; return true; }
};
$server->onMessage($connection, serialize([
    'cmd' => 'set',
    'key' => 'probe',
    'value' => new StorageWakeupProbe(),
]));
if (StorageWakeupProbe::$wokeUp) {
    throw new RuntimeException('Untrusted serialized object hooks must not execute.');
}

$trustedServer = new Server('127.0.0.1', 8008, allowObjectSerialization: true);
if ($serverProperty->getValue($trustedServer) !== true) {
    throw new RuntimeException('Server trusted-network opt-in was not retained.');
}

[$clientSocket, $serverSocket] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
$frameClient = new StorageClientProbe('127.0.0.1:8008');
$request = ['cmd' => 'set', 'key' => 'frame', 'value' => 'payload'];
$frameClient->writeFrame($request, $clientSocket);
$frame = fread($serverSocket, 65536);
$frameLength = unpack('Nlength', substr($frame, 0, 4))['length'];
if ($frameLength !== strlen($frame) - 4 || substr($frame, 4) !== serialize($request)) {
    throw new RuntimeException('Storage client frame length must describe only the payload.');
}

$response = serialize(['ok' => true]);
fwrite($serverSocket, pack('N', strlen($response)) . $response);
if ($frameClient->readFrame($clientSocket) !== ['ok' => true]) {
    throw new RuntimeException('Storage client failed to decode a Server 7 frame.');
}

fclose($clientSocket);
fclose($serverSocket);
echo "Storage serialization and frame regressions passed.\n";
