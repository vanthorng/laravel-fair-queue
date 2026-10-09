<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Contracts;

use Teerex\FairQueue\Partition;

interface PartitionResolver
{
    public function resolve(mixed $job, mixed $data = null): ?Partition;
}
