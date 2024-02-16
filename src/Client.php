<?php

namespace localzet\Storage;

/**
 *  Клиент глобального хранилища данных
 */
class Client
{
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
    public function __construct($servers)
    {
        if (empty($servers)) {
            throw new \Exception('Адрес хранилища пуст');
        }

        $this->_globalServers = array_values((array)$servers);
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
        $buffer = pack('N', 4 + strlen($buffer)) . $buffer;
        $len = fwrite($connection, $buffer);
        if ($len !== strlen($buffer)) {
            throw new \Exception('write fail');
        }
    }

    /**
     * Чтение
     * @throws Exception
     */
    protected function read($connection)
    {
        $all_buffer = '';
        $total_len = 4;
        $head_read = false;
        while (1) {
            $buffer = fread($connection, 8192);
            if ($buffer === '' || $buffer === false) {
                throw new \Exception('read fail');
            }
            $all_buffer .= $buffer;
            $recv_len = strlen($all_buffer);
            if ($recv_len >= $total_len) {
                if ($head_read) {
                    break;
                }
                $unpack_data = unpack('Ntotal_length', $all_buffer);
                $total_len = $unpack_data['total_length'];
                if ($recv_len >= $total_len) {
                    break;
                }
                $head_read = true;
            }
        }
        return unserialize(substr($all_buffer, 4));
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
