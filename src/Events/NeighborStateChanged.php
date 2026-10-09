<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Events;

final readonly class NeighborStateChanged
{
    public function __construct(
        public string $partition,
        public int $penaltyPercent,
        public string $reason,
    ) {}
}
