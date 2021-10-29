<?php

namespace Devim\Provider\RabbitmqServiceProvider;

use InvalidArgumentException;
use OldSound\RabbitMqBundle\RabbitMq\Producer;
use Pimple\Container;

use function array_key_exists;

/**
 * Class ProducerManager
 * @package Devim\Provider\RabbitmqServiceProvider
 */
class ProducerManager
{
    /**
     * @var Container
     */
    private $container;

    /**
     * @var Producer[]
     */
    private $producers = [];

    /**
     * ProducerManager constructor.
     * @param Container $container
     */
    public function __construct(Container $container)
    {
        $this->container = $container;
    }

    /**
     * @param string $name
     *
     * @return Producer
     * @throws InvalidArgumentException
     */
    public function getInstance(string $name): Producer
    {
        if (array_key_exists($name, $this->producers)) {
            return $this->producers[$name];
        }
        return $this->producers[$name] = $this->getProducer($name);
    }

    /**
     * @param string $name
     *
     * @return Producer
     * @throws InvalidArgumentException
     */
    private function getProducer(string $name): Producer
    {
        if (!array_key_exists($name, $this->container['rabbit.producers.config'])) {
            throw new InvalidArgumentException('Configuration for producer [' . $name . '] not found');
        }
        $options = $this->container['rabbit.producers.config'][$name];

        $nameConnection = $options['connection'] ?? RabbitServiceProvider::DEFAULT_CONNECTION;
        if (!isset($this->container['rabbit.connections'][$nameConnection])) {
            throw new InvalidArgumentException('Configuration for connection [' . $nameConnection . '] not found');
        }

        $connection = $this->container['rabbit.connection'][$nameConnection];
        $producer = new Producer($connection);
        $producer->setExchangeOptions($options['exchange_options']);
        //this producer doesn't define a queue
        if (!isset($options['queue_options'])) {
            $options['queue_options']['name'] = null;
        }
        $producer->setQueueOptions($options['queue_options']);
        if ((array_key_exists('auto_setup_fabric', $options)) && (!$options['auto_setup_fabric'])) {
            $producer->disableAutoSetupFabric();
        }
        return $producer;
    }
}
