<?php

declare(strict_types=1);

namespace Teerex\FairQueue;

use Illuminate\Contracts\Container\Container;
use Teerex\FairQueue\Contracts\PartitionedJob;
use Teerex\FairQueue\Contracts\PartitionResolver;

final readonly class ContextPartitionResolver implements PartitionResolver
{
    public function __construct(private Container $container) {}

    public function resolve(mixed $job, mixed $data = null): ?Partition
    {
        return $job instanceof PartitionedJob
            ? $job->fairQueuePartition()
            : $this->container->make(TenantContext::class)->current();
    }
}
