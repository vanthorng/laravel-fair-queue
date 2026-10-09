<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Scheduling;

use InvalidArgumentException;
use Teerex\FairQueue\Contracts\SchedulingStrategy;

final class StrategyFactory
{
    public static function make(mixed $value): SchedulingStrategy
    {
        if ($value instanceof SchedulingStrategy) {
            return $value;
        }

        return match ($value) {
            'rr' => new RoundRobin,
            'drr' => new DeficitRoundRobin,
            'wfq' => new WeightedFairQueuing,
            default => self::classStrategy($value),
        };
    }

    private static function classStrategy(mixed $value): SchedulingStrategy
    {
        if (! is_string($value) || ! is_subclass_of($value, SchedulingStrategy::class)) {
            throw new InvalidArgumentException('Fair queue strategy must be rr, drr, wfq, or a SchedulingStrategy class.');
        }

        return new $value;
    }
}
