<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Middleware;

use Teerex\FairQueue\Contracts\PartitionedJob;
use Teerex\FairQueue\TenantContext;

final readonly class RestorePartition
{
    public function __construct(private TenantContext $context) {}

    public function handle(object $job, callable $next): mixed
    {
        if (! $job instanceof PartitionedJob) {
            return $next($job);
        }

        return $this->context->runFor($job->fairQueuePartition(), fn () => $next($job));
    }
}
