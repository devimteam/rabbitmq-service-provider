<?php

namespace Devim\Provider\RabbitmqServiceProvider;

use OldSound\RabbitMqBundle\RabbitMq\BaseAmqp;
use OldSound\RabbitMqBundle\RabbitMq\BatchConsumer;
use OldSound\RabbitMqBundle\RabbitMq\Consumer;
use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use Pimple\Container;
use Pimple\ServiceProviderInterface;

class RabbitServiceProvider implements ServiceProviderInterface
{
    public const DEFAULT_CONNECTION = 'default';

    public function register(Container $pimple)
    {
        $this->loadConnections($pimple);
        $this->loadProducers($pimple);
        $this->loadConsumers($pimple);
    }

    private function loadConnections(Container $app): void
    {
        $app['rabbit.connection'] = function ($container) {
            if (!isset($container['rabbit.connections'])) {
                throw new \InvalidArgumentException('You need to specify at least a connection in your configuration.');
            }
            $connections = [];
            foreach ($container['rabbit.connections'] as $name => $options) {
                $isLazyConnection = $container['rabbit.connections'][$name]['lazy'] ?? false;
                $connectionClass  = $isLazyConnection ? AMQPLazyConnection::class : AMQPConnection::class;

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
            return new ProducerManager($app);
        };
    }

    private function loadConsumers(Container $app)
    {
        $app['rabbit.consumer'] = function ($app) {
            if (!isset($app['rabbit.consumers.config']) && !isset($app['rabbit.batch_consumers.config'])) {
                return null;
            }
            $consumers = [];

            foreach ($app['rabbit.consumers.config'] ?? [] as $name => $options) {
                $nameConnection = $options['connection'] ?? self::DEFAULT_CONNECTION;
                if (!isset($app['rabbit.connection'][$nameConnection])) {
                    throw new \InvalidArgumentException('Configuration for connection [' . $nameConnection . '] not found');
                }

                $connection = $app['rabbit.connection'][$nameConnection];
                $consumer   = new Consumer($connection);
                $this->setupConsumer($consumer, $app, $options);
                $consumers[$name] = $consumer;
            }

            foreach ($app['rabbit.batch_consumers.config'] ?? [] as $name => $options) {
                $nameConnection = $options['connection'] ?? self::DEFAULT_CONNECTION;
                if (!isset($app['rabbit.connection'][$nameConnection])) {
                    throw new \InvalidArgumentException('Configuration for connection [' . $nameConnection . '] not found');
                }

                $connection = $app['rabbit.connection'][$nameConnection];
                $consumer   = new BatchConsumer($connection);
                $this->setupConsumer($consumer, $app, $options);
                $consumers[$name] = $consumer;
            }


            return $consumers;
        };
    }

    private function setupConsumer(BaseAmqp $consumer, Container $app, array $options): void
    {
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
    }
}
