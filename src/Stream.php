<?php

namespace horizon\RedisStreamQueue;

use horizon\RedisStreamQueue\interface\StreamInterface;
use Illuminate\Redis\Connections\Connection;
use support\Log;
use support\Redis;
use Workerman\Timer;
use Workerman\Worker;

/**
 * Stream
 * @package horizon\RedisStreamQueue
 */
abstract class Stream implements StreamInterface
{
    /**
     * redis连接
     * @var string
     */
    protected string $connection = 'default';

    /**
     * 每次消费消息数量
     * @var int
     */
    protected int $prefetchCount = 1;

    /**
     * 单次消费超时时间
     * @var int
     */
    protected int $pendingThreshold = 10;

    /**
     * 重试次数检查
     * @var int
     */
    protected int $deliveryFailCount = 5;

    /**
     * 检查无消息后会暂停拉取的时间
     * @var int
     */
    protected int $brock = 10;

    /**
     * 队列业务名
     * @var string
     */
    protected string $name = '';

    /**
     * redis stream 队列
     * @var string
     */
    protected string $stream = '';

    /**
     * redis stream 失败队列
     * @var string
     */
    protected string $failStream = '';

    /**
     * 消费者组
     * @var string
     */
    protected string $group = '';

    /**
     * @var Connection|null
     */
    private ?Connection $_connection;

    /**
     * @var array|null
     */
    private ?array $_timer = null;


    protected static array $_consumers = [];

    public function __construct()
    {
        $this->_connection = Redis::connection($this->connection);
        $this->name = str_replace('\\', '_', static::class);
        $this->stream = 'stream:'.$this->name;
        $this->failStream = 'stream:fail:'.$this->name;
        $this->group = 'stream:consumer_group:'.$this->name;
    }

    /**
     * instance
     * @return Stream
     */
    public static function instance()
    {
        $class = static::class;
        return self::$_consumers[$class] ?? new $class();
    }

    /**
     * 进程启动自动消费
     * @param Worker $worker
     * @return void
     * @throws \RedisException
     */
    public function onWorkerStart(Worker $worker): void
    {
        $interval = 0.001;
        $client = $this->connection()->client();
        $consumer = gethostname().'_worker'.$worker->id;

        //消费者组
        $client->xGroup(
            'CREATE',
            $this->stream,
            $this->group,
            '0',
            true
        );

        //失败队列消费者组
        $client->xGroup(
            'CREATE',
            $this->failStream,
            $this->group,
            '0',
            true
        );

        $this->_timer['consumer'] = Timer::add($interval, function () use ($client , $interval , $consumer){
            //读取未ack的消息
            while($res = $client->xReadGroup($this->group, $consumer, [$this->stream => '>'], $this->prefetchCount, $this->brock))
            {
                $list = current($res);
                // 信息组
                foreach ($list as $id => $data){
                    try {
                        //消费回调consumer
                        if($this->consumer($id, $data, $this->connection())){
                            //确认消费
                            $client->xAck($this->stream, $this->group, [$id]);
                            //删除ack的消息
                            $client->xDel($this->stream,[$id]);
                        }
                    }catch (\Throwable $throwable){
                        Log::channel('stream')->error($throwable->getMessage(), ['data' => $data,'trace' => $throwable->getTrace()]);
                    }
                }
            }
        });

        $this->_timer['check_pending'] = Timer::add(3, function () use ($consumer, $client){
            $pendingMessages = $client->xpending($this->stream, $this->group, '-', '+', 10);
            // 处理挂起的消息
            if (!empty($pendingMessages)) foreach ($pendingMessages as $pendingMessage) {
                [ $id ,  , $elapsedTime , $deliveryCount ] = $pendingMessage;
                //$id , $pendingConsumer , $elapsedTime , $deliveryCount
                // 如果挂起时间超过阈值，尝试重新分配消息
                if ($elapsedTime > $this->pendingThreshold) {
                    //重新投递
                    $res = $client->XCLAIM($this->stream, $this->group, $consumer, 3000, [$id]);
                    //失败次数超过限制值抛弃消息不再投递
                    if ($deliveryCount >= $this->deliveryFailCount){
                        //失败推送失败队列
                        if (!empty($res)){
                            $client->xAdd($this->failStream,'*', current($res));
                        }
                        $client->xDel($this->stream,[$id]);
                    }elseif(!empty($res)){
                        try {
                            //消费回调consumer
                            if($this->consumer($id, current($res), $this->connection())){
                                //确认消费
                                $client->xAck($this->stream, $this->group, [$id]);
                                //删除ack的消息
                                $client->xDel($this->stream,[$id]);
                            }
                        }catch (\Throwable $throwable){}
                    }
                }
            }
        });
    }

    /**
     * @param Worker $worker
     * @return void
     * @throws \RedisException
     */
    public function onWorkerStop(Worker $worker): void
    {
        if($this->_connection){
            $this->_connection->client()->close();
            $this->_connection = null;
        }

        if($this->_timer){
            foreach ($this->_timer as $t){
                Timer::del($t);
            }
            $this->_timer = null;
        }
    }

    /**
     * 获取连接
     * @return Connection
     */
    public function connection() : Connection
    {
        if(!$this->_connection instanceof Connection){
            $this->_connection = Redis::connection($this->connection);
        }
        return $this->_connection;
    }

    /**
     * 生产者
     * @param array $body
     * @return \Redis|string
     * @throws \RedisException
     */
    public function producer (array $body) : string|\Redis
    {
        return $this->connection()->client()->xAdd($this->stream,'*', $body);
    }

    /**
     * 根据messageId获取消息时间戳
     * @param string $msgId
     * @return int
     */
    public function getMsgTimestamp (string $msgId) : int
    {
        return floor((current(explode('-' , $msgId)))/1000);
    }
}