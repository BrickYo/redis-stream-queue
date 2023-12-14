<?php

namespace horizon\RedisStreamQueue\interface;

use Illuminate\Redis\Connections\Connection;
use Workerman\Worker;

/**
 * Interface StreamInterface
 * @package horizon\RedisStreamQueue\interface
 */
interface StreamInterface
{
    public function onWorkerStart (Worker $worker);

    public function onWorkerStop (Worker $worker);

    public function connection ();

    public function producer(array $body);

    public function consumer (string $msgId, array $data, Connection $connection);
}