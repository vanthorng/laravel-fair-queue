<?php

declare(strict_types=1);

namespace Teerex\FairQueue;

use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Psr\Log\LoggerInterface;
use Teerex\FairQueue\Contracts\PartitionResolver;

final readonly class FairQueueConnector implements ConnectorInterface
{
    /** @param array<string, mixed> $defaults */
    public function __construct(
        private RedisFactory $redis,
        private PartitionResolver $resolver,
        private Dispatcher $events,
        private LoggerInterface $logger,
        private array $defaults,
    ) {}

    /** @param array<string, mixed> $config */
    public function connect(array $config)
    {
        $settings = array_replace_recursive($this->defaults, $config);

        return new FairRedisQueue(
            $this->redis,
            $this->resolver,
            $this->events,
            $this->logger,
            $settings,
            (string) ($config['queue'] ?? $this->defaults['queue'] ?? 'default'),
            (string) ($config['connection'] ?? $this->defaults['redis_connection'] ?? 'default'),
            (int) ($config['retry_after'] ?? $this->defaults['retry_after'] ?? 120),
            (bool) ($config['after_commit'] ?? false),
        );
    }
}
