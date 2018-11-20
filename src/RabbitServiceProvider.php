<?php
namespace Devim\Provider\RabbitmqServiceProvider;

use Pimple\Container;
use Pimple\ServiceProviderInterface;
use OldSound\RabbitMqBundle\RabbitMq\Producer;
use OldSound\RabbitMqBundle\RabbitMq\Consumer;
use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Connection\AMQPLazyConnection;

class RabbitServiceProvider implements ServiceProviderInterface
{
    private const DEFAULT_CONNECTION = 'default';

    public function register(Container $pimple)
    {
        $this->loadConnections($pimple);
        $this->loadProducers($pimple);
        $this->loadConsumers($pimple);
    }

    private function loadConnections(Container $app): void
    {
        $container['rabbit.connection'] = function ($container) {
            if (!isset($container['rabbit.connections'])) {
                throw new \InvalidArgumentException('You need to specify at least a connection in your configuration.');
            }
            $connections = [];
            foreach ($container['rabbit.connections'] as $name => $options) {
                $isLazyConnection = $container['rabbit.connections'][$name]['lazy'] ?? false;
                $connectionClass = $isLazyConnection ? AMQPLazyConnection::class : AMQPConnection::class;

                $connection = new $connectionClass(
                    $container['rabbit.connections'][$name]['host'],
                    $container['rabbit.connections'][$name]['port'],
                    $container['rabbit.connections'][$name]['user'],
                    $container['rabbit.connections'][$name]['password'],
                    $container['rabbit.connections'][$name]['vhost']
                );

                $connections[$name] = $connection;
            }

            return $connections;
        };
    }

    private function loadProducers(Container $app): void
    {
        $app['rabbit.producer'] = function ($app) {
            if (!isset($app['rabbit.producers'])) {
                return null;
            }
            $producers = [];
            foreach ($app['rabbit.producers'] as $name => $options) {
                $nameConnection = $options['connection'] ?? self::DEFAULT_CONNECTION;
                if (!isset($app['rabbit.connection'][$nameConnection])) {
                    throw new \InvalidArgumentException('Configuration for connection [' . $nameConnection . '] not found');
                }

                $connection = $app['rabbit.connection'][$nameConnection];
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
                $producers[$name] = $producer;
            }

            return $producers;
        };
    }

    private function loadConsumers(Container $app)
    {
        $app['rabbit.consumer'] = function ($app) {
            if (!isset($app['rabbit.consumers'])) {
                return null;
            }
            $consumers = [];

            foreach ($app['rabbit.consumers'] as $name => $options) {
                $nameConnection = $options['connection'] ?? self::DEFAULT_CONNECTION;
                if (!isset($app['rabbit.connection'][$nameConnection])) {
                    throw new \InvalidArgumentException('Configuration for connection [' . $nameConnection . '] not found');
                }

                $connection = $app['rabbit.connection'][$nameConnection];
                $consumer = new Consumer($connection);
                $consumer->setExchangeOptions($options['exchange_options']);
                $consumer->setQueueOptions($options['queue_options']);
                $consumer->setCallback(array($app[$options['callback']], 'execute'));
                if (array_key_exists('qos_options', $options)) {
                    $consumer->setQosOptions(
                        $options['qos_options']['prefetch_size'],
                        $options['qos_options']['prefetch_count'],
                        $options['qos_options']['global']
                    );
                }
                if (array_key_exists('idle_timeout', $options)) {
                    $consumer->setIdleTimeout($options['idle_timeout']);
                }
                if ((array_key_exists('auto_setup_fabric', $options)) && (!$options['auto_setup_fabric'])) {
                    $consumer->disableAutoSetupFabric();
                }
                $consumers[$name] = $consumer;
            }

            return $consumers;
        };
    }
}