<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Tests\Unit;

use PHPUnit\Framework\TestCase;
use Teerex\FairQueue\Partition;
use Teerex\FairQueue\Scheduling\DeficitRoundRobin;
use Teerex\FairQueue\Scheduling\RoundRobin;
use Teerex\FairQueue\Scheduling\StrategyFactory;
use Teerex\FairQueue\Scheduling\WeightedFairQueuing;

final class PartitionTest extends TestCase
{
    public function test_partition_keys_are_kind_scoped(): void
    {
        self::assertNotSame((new Partition('tenant', '42'))->key(), (new Partition('group', '42'))->key());
    }

    public function test_empty_partition_is_rejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new Partition('tenant', '');
    }

    public function test_builtin_strategies_are_resolvable(): void
    {
        self::assertInstanceOf(RoundRobin::class, StrategyFactory::make('rr'));
        self::assertInstanceOf(DeficitRoundRobin::class, StrategyFactory::make('drr'));
        self::assertInstanceOf(WeightedFairQueuing::class, StrategyFactory::make('wfq'));
    }
}
