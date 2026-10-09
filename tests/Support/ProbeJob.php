<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Tests\Support;

use Illuminate\Contracts\Queue\ShouldQueue;
use Teerex\FairQueue\Contracts\PartitionedJob;
use Teerex\FairQueue\Partition;

final class ProbeJob implements PartitionedJob, ShouldQueue
{
    public static int $executed = 0;

    public function __construct(public string $tenant) {}

    public function fairQueuePartition(): Partition
    {
        return new Partition('tenant', $this->tenant);
    }

    public function handle(): void
    {
        self::$executed++;
    }
}
