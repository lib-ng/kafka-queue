<?php

namespace Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{
    
    public function connect(array $config)
    {
        $producer = $this->createProducer($config);
        $consumer = $this->createConsumer($config);  
        return new KafkaQueue($producer, $consumer);
    }

    public function createProducer(array $config)
    {          
        $conf = $this->setCommonParameters($config);
        $conf->set('message.send.max.retries', 5);
        $producer = new \RdKafka\Producer($conf);
        // dump("Producer created");
        $producer->setLogLevel(LOG_DEBUG);
        return $producer;
    }
    public function createConsumer(array $config)
    {          
        $conf = $this->setCommonParameters($config);
        $conf->set('group.id', $config['group_id']);
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new \RdKafka\KafkaConsumer($conf);        
        // dump("Consumer created");

        $topic = $config['queue'];
        $consumer->subscribe([$topic]);
        // dump("Consumer subscribed to topic - $topic");
        return $consumer;
    }

    public function setCommonParameters(array $config)
    {
        $conf = new \RdKafka\conf();
        $conf->set('bootstrap.servers', $config['bootstrap_servers']);
        $conf->set('security.protocol', $config['security_protocol']);
        $conf->set('sasl.mechanisms', $config['sasl_mechanisms']);
        $conf->set('sasl.username', $config['sasl_username']);
        $conf->set('sasl.password', $config['sasl_password']);  
        return $conf;
    }

}