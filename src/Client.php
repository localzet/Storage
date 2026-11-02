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

namespace localzet\Storage;

/**
 *  Клиент глобального хранилища данных
 */
class Client
{
    private const MAX_FRAME_SIZE = 64 * 1024 * 1024;

    /** @var bool */
    private bool $allowObjectSerialization;
    /**
     * Таймаут
     * @var int
     */
    public $timeout = 5;

    /**
     * Интервал серцебиения
     * @var int
     */
    public $pingInterval = 25;

    /**
     * Адреса глобального хранилища
     * @var array
     */
    protected $_globalServers = array();

    /**
     * Соединения с глобальным сервером
     * @var resource
     */
    protected $_globalConnections = null;

    /**
     * Кэш
     * @var array
     */
    protected $_cache = array();

    /**
     * @param array|string $servers
     */
    public function __construct($servers, bool $allowObjectSerialization = false)
    {
        if (empty($servers)) {
            throw new \Exception('Адрес хранилища пуст');
        }

        $this->_globalServers = array_values((array)$servers);
        $this->allowObjectSerialization = $allowObjectSerialization;
    }

    /**
     * Подключение
     * @throws \Exception
     */
    protected function connect($key)
    {
        $offset = crc32($key) % count($this->_globalServers);
        if ($offset < 0) {
            $offset = -$offset;
        }

        if (!isset($this->_globalConnections[$offset]) || !is_resource($this->_globalConnections[$offset]) || feof($this->_globalConnections[$offset])) {
            $connection = stream_socket_client("tcp://{$this->_globalServers[$offset]}", $code, $msg, $this->timeout);
            if (!$connection) {
                throw new \Exception($msg);
            }

            stream_set_timeout($connection, $this->timeout);
            if (class_exists('\localzet\Timer') && php_sapi_name() === 'cli') {
                $timer_id = \localzet\Timer::add($this->pingInterval, function ($connection) use (&$timer_id) {
                    $buffer = pack('N', 8) . "ping";
                    if (strlen($buffer) !== @fwrite($connection, $buffer)) {
                        @fclose($connection);
                        \localzet\Timer::del($timer_id);
                    }
                }, array($connection));
            }
            $this->_globalConnections[$offset] = $connection;
        }
        return $this->_globalConnections[$offset];
    }

    /**
     * Запись
     * @param string $buffer
     */
    protected function write($data, $connection)
    {
        $buffer = serialize($data);
        $length = strlen($buffer);
        if ($length === 0 || $length > self::MAX_FRAME_SIZE) {
            throw new \LengthException('Storage frame size is invalid.');
        }

        $buffer = pack('N', $length) . $buffer;
        $offset = 0;
        $bufferLength = strlen($buffer);
        while ($offset < $bufferLength) {
            $written = fwrite($connection, substr($buffer, $offset));
            if ($written === false || $written === 0) {
                throw new \RuntimeException('Storage frame write failed.');
            }
            $offset += $written;
        }
    }

    /**
     * Чтение
     * @throws Exception
     */
    protected function read($connection)
    {
        $header = $this->readExact($connection, 4);
        $length = unpack('Nlength', $header)['length'];
        if ($length === 0 || $length > self::MAX_FRAME_SIZE) {
            throw new \LengthException('Storage frame size is invalid.');
        }

        $payload = $this->readExact($connection, $length);
        return unserialize($payload, ['allowed_classes' => $this->allowObjectSerialization]);
    }

    private function readExact($connection, int $length): string
    {
        $buffer = '';
        while (strlen($buffer) < $length) {
            $chunk = fread($connection, $length - strlen($buffer));
            if ($chunk === false || $chunk === '') {
                $metadata = stream_get_meta_data($connection);
                $reason = ($metadata['timed_out'] ?? false) ? 'timed out' : 'closed before the frame was complete';
                throw new \RuntimeException('Storage frame read ' . $reason . '.');
            }
            $buffer .= $chunk;
        }

        return $buffer;
    }

    /**-------------------------------------------------------------------------------------------------- */

    /**
     * Установка значения
     * @param string $key
     * @param mixed $value
     * @throws \Exception
     */
    public function __set($key, $value)
    {
        return $this->set($key, $value);
    }

    /**
     * Проверка данных
     * @param string $key
     */
    public function __isset($key)
    {
        return null !== $this->__get($key);
    }

    /**
     * Удаление данных
     * @param string $key
     * @throws \Exception
     */
    public function __unset($key)
    {
        return $this->delete($key);
    }

    /**
     * Получение данных
     * @param string $key
     * @throws \Exception
     */
    public function __get($key)
    {
        return $this->get($key);
    }

    /**-------------------------------------------------------------------------------------------------- */

    /**
     * Установка значения
     * @param string $key
     * @param mixed $value
     * @throws \Exception
     */
    public function set($key, $value)
    {
        $connection = $this->connect($key);
        $this->write(array(
            'cmd'       => 'set',
            'key'       => $key,
            'value'     => $value,
        ), $connection);
        return $this->read($connection);
    }

    /**
     * Получение данных
     * @param string $key
     * @throws \Exception
     */
    public function get($key)
    {
        $connection = $this->connect($key);
        $this->write(array(
            'cmd'       => 'get',
            'key'       => $key,
        ), $connection);
        return $this->read($connection);
    }

    /**
     * Обновление после проверки старого значения
     * @param string $key
     * @param mixed $value
     * @throws \Exception
     */
    public function cas($key, $old_value, $new_value)
    {
        $connection = $this->connect($key);
        $this->write(array(
            'cmd'       => 'cas',
            'md5'       => md5(serialize($old_value)),
            'key'       => $key,
            'value'     => $new_value,
        ), $connection);
        return $this->read($connection);
    }

    /**
     * Добавление данных
     * @param string $key
     * @throws \Exception
     */
    public function add($key, $value)
    {
        $connection = $this->connect($key);
        $this->write(array(
            'cmd'       => 'add',
            'key'       => $key,
            'value'     => $value,
        ), $connection);
        return $this->read($connection);
    }

    /**
     * Инкрементирование числа
     * @param string $key
     * @throws \Exception
     */
    public function increment($key, $step = 1)
    {
        $connection = $this->connect($key);
        $this->write(array(
            'cmd'       => 'increment',
            'key'       => $key,
            'step'      => $step,
        ), $connection);
        return $this->read($connection);
    }

    /**
     * Удаление данных
     * @param string $key
     * @throws \Exception
     */
    public function delete($key)
    {
        $connection = $this->connect($key);
        $this->write(array(
            'cmd'       => 'delete',
            'key'       => $key
        ), $connection);
        return $this->read($connection);
    }
}
