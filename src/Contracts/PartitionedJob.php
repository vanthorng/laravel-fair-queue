<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Contracts;

use Teerex\FairQueue\Partition;

interface PartitionedJob
{
    public function fairQueuePartition(): Partition;
}
