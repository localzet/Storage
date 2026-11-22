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
use localzet\Storage\SerializationGuard;
use localzet\Storage\Server;

final class StorageWakeupProbe
{
    public static bool $wokeUp = false;

    public function __wakeup(): void
    {
        self::$wokeUp = true;
    }
}

final class StorageWakeupWarningProbe
{
    public function __wakeup(): void
    {
        trigger_error('trusted wakeup warning', E_USER_WARNING);
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
$coreServer = $coreServerProperty->getValue($server);
if ($coreServer->getSocketName() !== 'frame://127.0.0.1:8008') {
    throw new RuntimeException('Storage server must bind to loopback by default.');
}
$connectionPair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
if ($connectionPair === false) {
    throw new RuntimeException('Could not create a Storage connection test pair.');
}
$connectionProbe = new \localzet\Server\Connection\TcpConnection(
    new \localzet\Server\Events\Select(),
    $connectionPair[0]
);
($coreServer->onConnect)($connectionProbe);
if ($connectionProbe->maxPackageSize !== Client::MAX_FRAME_SIZE + Client::FRAME_HEADER_SIZE
    || $connectionProbe->maxSendBufferSize !== Client::MAX_FRAME_SIZE + Client::FRAME_HEADER_SIZE
) {
    throw new RuntimeException('Storage transport limits must include the complete frame header and payload.');
}
$maximumFrameLength = \localzet\Server\Protocols\Frame::input(
    pack('N', Client::MAX_FRAME_SIZE),
    $connectionProbe
);
if ($maximumFrameLength > $connectionProbe->maxPackageSize) {
    throw new RuntimeException('Storage must accept the largest supported payload with its frame header.');
}
$connectionProbe->destroy();
fclose($connectionPair[1]);
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
$dataProperty = new ReflectionProperty($server, '_dataArray');
$dataProperty->setAccessible(true);
if (StorageWakeupProbe::$wokeUp || isset($dataProperty->getValue($server)['probe'])) {
    throw new RuntimeException('Untrusted object payload must not execute hooks or enter Storage.');
}

$server->onMessage($connection, serialize([
    'cmd' => 'set',
    'key' => 'nested-probe',
    'value' => ['nested' => new StorageWakeupProbe()],
]));
if (isset($dataProperty->getValue($server)['nested-probe'])) {
    throw new RuntimeException('Nested untrusted object payload must not enter Storage.');
}

$server->onMessage($connection, serialize(new StorageWakeupProbe()));
if (StorageWakeupProbe::$wokeUp) {
    throw new RuntimeException('Root untrusted object payload must not execute hooks.');
}

$acceptedValues = ['scalar' => 'value', 'number' => 42, 'nested' => ['null' => null, 'flag' => true]];
$server->onMessage($connection, serialize([
    'cmd' => 'set',
    'key' => 'accepted',
    'value' => $acceptedValues,
]));
if (($dataProperty->getValue($server)['accepted'] ?? null) !== $acceptedValues) {
    throw new RuntimeException('Scalar and nested-array payloads must remain supported.');
}

$serializedValues = [
    null,
    false,
    PHP_INT_MIN,
    1.25,
    NAN,
    INF,
    -INF,
    "quote\"; brace{} and NUL\0 byte",
    ['nested' => [1, 2.5, false]],
];
foreach ($serializedValues as $value) {
    if (!SerializationGuard::supportsSerializedValue(serialize($value))) {
        throw new RuntimeException('Serialized scalar and array values must pass preflight.');
    }
}
if (SerializationGuard::supportsSerializedValue(serialize(new StorageWakeupProbe()))) {
    throw new RuntimeException('Serialized object payloads must fail preflight before decoding.');
}
foreach (['', 'i:1', 'a:1:{i:0;O:8:"stdClass":0:{}}'] as $invalidPayload) {
    if (SerializationGuard::supportsSerializedValue($invalidPayload)) {
        throw new RuntimeException('Malformed or object payloads must fail serialization preflight.');
    }
}

$malformedReference = 'a:3:{s:3:"cmd";s:3:"set";s:3:"key";s:1:"x";s:5:"value";R:999;}';
$warnings = [];
set_error_handler(static function (int $severity, string $message) use (&$warnings): bool {
    $warnings[] = $message;

    return true;
});
try {
    $server->onMessage($connection, $malformedReference);
} finally {
    restore_error_handler();
}
if ($warnings !== [] || end($connection->sent) !== serialize('bad request')) {
    throw new RuntimeException('Malformed references must be rejected without emitting PHP warnings.');
}

$cyclic = [];
$cyclic['self'] =& $cyclic;
if (SerializationGuard::supports($cyclic)) {
    throw new RuntimeException('Recursive arrays must be rejected without recursive traversal.');
}
$shared = ['value' => 'shared'];
$sharedAcyclicValues = [];
$sharedAcyclicValues['first'] =& $shared;
$sharedAcyclicValues['second'] =& $shared;
if (!SerializationGuard::supports($sharedAcyclicValues)) {
    throw new RuntimeException('Repeated acyclic references must remain supported.');
}
$atValueLimit = range(1, 100_000);
if (!SerializationGuard::supports($atValueLimit)) {
    throw new RuntimeException('Arrays within the validation value limit must remain supported.');
}
$requestAtValueLimit = ['cmd' => 'set', 'key' => 'at-limit', 'value' => $atValueLimit];
if (!SerializationGuard::supportsRequest($requestAtValueLimit)
    || !SerializationGuard::supportsSerializedRequest(serialize($requestAtValueLimit))
) {
    throw new RuntimeException('Storage requests within the value limit must remain supported.');
}
$tooManyValues = range(1, 100_001);
if (SerializationGuard::supports($tooManyValues)) {
    throw new RuntimeException('Arrays over the validation value limit must be rejected.');
}
if (SerializationGuard::supportsSerializedValue(serialize($tooManyValues))) {
    throw new RuntimeException('Serialized arrays over the validation value limit must fail preflight.');
}
$requestOverValueLimit = ['cmd' => 'set', 'key' => 'over-limit', 'value' => $tooManyValues];
if (SerializationGuard::supportsRequest($requestOverValueLimit)
    || SerializationGuard::supportsSerializedRequest(serialize($requestOverValueLimit))
) {
    throw new RuntimeException('Storage requests over the value limit must be rejected.');
}
$server->onMessage($connection, serialize([
    'cmd' => 'set',
    'key' => 'too-many-values',
    'value' => $tooManyValues,
]));
if (isset($dataProperty->getValue($server)['too-many-values'])) {
    throw new RuntimeException('Storage server must reject arrays over the validation value limit.');
}
$atDepthLimit = 'leaf';
for ($depth = 0; $depth < 128; ++$depth) {
    $atDepthLimit = [$atDepthLimit];
}
if (!SerializationGuard::supports($atDepthLimit)) {
    throw new RuntimeException('Arrays within the validation depth limit must remain supported.');
}
$requestAtDepthLimit = ['cmd' => 'set', 'key' => 'at-depth-limit', 'value' => $atDepthLimit];
if (!SerializationGuard::supportsRequest($requestAtDepthLimit)
    || !SerializationGuard::supportsSerializedRequest(serialize($requestAtDepthLimit))
) {
    throw new RuntimeException('Storage requests within the validation depth limit must remain supported.');
}
$tooDeep = 'leaf';
for ($depth = 0; $depth < 129; ++$depth) {
    $tooDeep = [$tooDeep];
}
if (SerializationGuard::supports($tooDeep)) {
    throw new RuntimeException('Arrays over the validation depth limit must be rejected.');
}
$requestOverDepthLimit = ['cmd' => 'set', 'key' => 'over-depth-limit', 'value' => $tooDeep];
if (SerializationGuard::supportsRequest($requestOverDepthLimit)
    || SerializationGuard::supportsSerializedRequest(serialize($requestOverDepthLimit))
) {
    throw new RuntimeException('Storage requests over the validation depth limit must be rejected.');
}
$server->onMessage($connection, serialize([
    'cmd' => 'set',
    'key' => 'too-deep',
    'value' => $tooDeep,
]));
if (isset($dataProperty->getValue($server)['too-deep'])) {
    throw new RuntimeException('Storage server must reject arrays over the validation depth limit.');
}
$server->onMessage($connection, serialize([
    'cmd' => 'set',
    'key' => 'cycle',
    'value' => $cyclic,
]));
if (isset($dataProperty->getValue($server)['cycle'])) {
    throw new RuntimeException('Storage server must reject recursive arrays.');
}

$trustedServer = new Server('127.0.0.1', 8008, allowObjectSerialization: true);
if ($serverProperty->getValue($trustedServer) !== true) {
    throw new RuntimeException('Server trusted-network opt-in was not retained.');
}
$trustedServer->onMessage($connection, serialize([
    'cmd' => 'set',
    'key' => 'trusted-object',
    'value' => new StorageWakeupProbe(),
]));
$trustedValue = $dataProperty->getValue($trustedServer)['trusted-object'] ?? null;
if (!StorageWakeupProbe::$wokeUp || !($trustedValue instanceof StorageWakeupProbe)) {
    throw new RuntimeException('Trusted-network opt-in must preserve object payload compatibility.');
}

$trustedWarnings = [];
set_error_handler(static function (int $severity, string $message) use (&$trustedWarnings): bool {
    $trustedWarnings[] = $message;

    return true;
});
try {
    $trustedWarningValue = SerializationGuard::decode(serialize(new StorageWakeupWarningProbe()), true);
} finally {
    restore_error_handler();
}
if (!($trustedWarningValue instanceof StorageWakeupWarningProbe)
    || $trustedWarnings !== ['trusted wakeup warning']
) {
    throw new RuntimeException('Trusted object wakeup warnings must retain native unserialize behavior.');
}

[$clientSocket, $serverSocket] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
$frameClient = new StorageClientProbe('127.0.0.1:8008');
$rejectedObject = false;
try {
    $frameClient->writeFrame(['cmd' => 'set', 'key' => 'object', 'value' => new StorageWakeupProbe()], $clientSocket);
} catch (InvalidArgumentException) {
    $rejectedObject = true;
}
if (!$rejectedObject) {
    throw new RuntimeException('Storage client must reject object payloads without opt-in.');
}
$rejectedLargeArray = false;
try {
    $frameClient->writeFrame(['cmd' => 'set', 'key' => 'large', 'value' => $tooManyValues], $clientSocket);
} catch (InvalidArgumentException) {
    $rejectedLargeArray = true;
}
if (!$rejectedLargeArray) {
    throw new RuntimeException('Storage client must reject arrays over the validation value limit.');
}

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

$response = serialize(['nested' => new StorageWakeupProbe()]);
fwrite($serverSocket, pack('N', strlen($response)) . $response);
$rejectedResponse = false;
try {
    $frameClient->readFrame($clientSocket);
} catch (UnexpectedValueException) {
    $rejectedResponse = true;
}
if (!$rejectedResponse) {
    throw new RuntimeException('Storage client must reject object responses without opt-in.');
}

$trustedFrameClient = new StorageClientProbe('127.0.0.1:8008', allowObjectSerialization: true);
$response = serialize(['nested' => new StorageWakeupProbe()]);
fwrite($serverSocket, pack('N', strlen($response)) . $response);
$trustedResponse = $trustedFrameClient->readFrame($clientSocket);
if (!($trustedResponse['nested'] instanceof StorageWakeupProbe)) {
    throw new RuntimeException('Storage client trusted-network opt-in must preserve object responses.');
}

fclose($clientSocket);
fclose($serverSocket);
echo "Storage serialization and frame regressions passed.\n";
