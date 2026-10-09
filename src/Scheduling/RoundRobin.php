<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Scheduling;

final readonly class RoundRobin extends BuiltinStrategy
{
    public function mode(): string
    {
        return 'rr';
    }
}
