<?php

namespace localzet\Storage;

use localzet\Server as WebCore;

/**
 * Сервер глобального хранилища данных
 */
class Server
{
    /**
     * Экземпляр WebCore
     * @var server
     */
    protected $_server = null;

    /**
     * Хранилище данных
     * @var array
     */
    protected $_dataArray = array();

    /**
     * @param string $ip
     * @param int $port
     */
    public function __construct($ip = '0.0.0.0', $port = 8008)
    {
        $server = new WebCore("frame://$ip:$port");
        $server->count = 1;
        $server->name = 'StorageServer';
        $server->onMessage = array($this, 'onMessage');
        $server->reloadable = false;
        $this->_server = $server;
    }

    /**
     * @param TcpConnection $connection
     * @param string $buffer
     */
    public function onMessage($connection, $buffer)
    {
        if ($buffer === 'ping') {
            return;
        }
        $data = unserialize($buffer);
        if (!$buffer || !isset($data['cmd']) || !isset($data['key'])) {
            // В запресе должны быть 'cmd' и 'key'
            return $connection->close(serialize('bad request'));
        }

        $cmd = $data['cmd'];
        $key = $data['key'];
        $value = isset($data['value']) ? $data['value'] : null;
        $step = isset($data['step']) ? $data['step'] : null;
        $md5 = isset($data['md5']) ? $data['md5'] : null;

        switch ($cmd) {
            case 'get':
                // Получение данных
                // $data = [ 'cmd' => 'get', 'key' => ??? ]

                if (!isset($this->_dataArray[$key])) {
                    return $connection->send('N;');
                }

                return $connection->send(serialize($this->_dataArray[$key]));

                break;
            case 'set':
                // Установка значения
                // $data = [ 'cmd' => 'set', 'key' => ???, 'value' => ??? ]

                $this->_dataArray[$key] = $value;
                $connection->send('b:1;');

                break;
            case 'add':
                // Добавление данных
                // $data = [ 'cmd' => 'add', 'key' => ???, 'value' => ??? ]

                if (isset($this->_dataArray[$key])) {
                    return $connection->send('b:0;');
                }

                $this->_dataArray[$key] = $value;
                return $connection->send('b:1;');

                break;
            case 'increment':
                // Инкрементирование числа
                // $data = [ 'cmd' => 'increment', 'key' => ???, 'step' => ??? ]

                if (!isset($this->_dataArray[$key])) {
                    return $connection->send('b:0;');
                }
                if (!is_numeric($this->_dataArray[$key])) {
                    $this->_dataArray[$key] = 0;
                }

                $this->_dataArray[$key] = $this->_dataArray[$key] + $step;
                return $connection->send(serialize($this->_dataArray[$key]));

                break;
            case 'cas':
                // Для защищённых данных (Обновление после проверки md5 старого значения)
                // $data = [ 'cmd' => 'cas', 'key' => ???, 'md5' => ???, 'value' => ??? ]

                $old_value = !isset($this->_dataArray[$key]) ? null : $this->_dataArray[$key];
                if (md5(serialize($old_value)) === $md5) {
                    $this->_dataArray[$key] = $value;
                    return $connection->send('b:1;');
                }
                $connection->send('b:0;');

                break;
            case 'delete':
                // Удаление данных
                // $data = [ 'cmd' => 'delete', 'key' => ??? ]
                unset($this->_dataArray[$key]);
                $connection->send('b:1;');

                break;
            default:
                // Некорректная команда
                $connection->close(serialize('bad cmd ' . $cmd));
        }
    }
}
