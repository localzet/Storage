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

use localzet\Server as WebCore;

/**
 * Сервер глобального хранилища данных
 */
class Server
{
    /** @var bool */
    private bool $allowObjectSerialization;
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
    public function __construct($ip = '127.0.0.1', $port = 8008, bool $allowObjectSerialization = false)
    {
        $this->allowObjectSerialization = $allowObjectSerialization;
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
        try {
            $data = unserialize($buffer, ['allowed_classes' => $this->allowObjectSerialization]);
        } catch (\Throwable) {
            return $connection->close(serialize('bad request'));
        }
        if (!$buffer || !is_array($data) || !isset($data['cmd']) || !isset($data['key'])) {
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
