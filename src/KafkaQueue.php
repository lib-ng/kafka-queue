<?php

namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;

class KafkaQueue extends Queue implements QueueContract
{
    protected $producer, $consumer;

    public function __construct($producer, $consumer)
    {
        $this->producer = $producer;
        $this->consumer = $consumer;
    }

    public function later($delay, $job, $data = '', $queue = null)
    {

    }
   
    public function pop($queue = null)
    {     
        try{
            $message = $this->consumer->consume(300 * 1000);

            switch($message->err){
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $job = unserialize($message->payload);
                    $job->handle();
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    dump("No more messages; will wait for more.");
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    dump("Time out.");
                    break;                
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            } 
        }
        catch (\Exception $e) {
            dump($e.getMessage());
        }
    }


    public function push($job, $data = '', $queue = null)
    {
        $topic_name = $queue ?? config('queue.connections.kafka.queue');
        $kafka_topic = $this->producer->newTopic($topic_name);
        $a = $kafka_topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));

        $this->producer->poll(0);
        while ($this->producer->getOutQLen() > 0) {
            $this->producer->poll(50);
        }
        // dump("Successful produced message for topic - $topic_name");

        $this->producer->flush(1000);
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {

    }

    public function size($queue = null)
    {     
          
    }



}
