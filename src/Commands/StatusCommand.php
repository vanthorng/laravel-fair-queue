<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Commands;

use Illuminate\Console\Command;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Teerex\FairQueue\FairRedisQueue;

final class StatusCommand extends Command
{
    protected $signature = 'fair-queue:status {--connection=fair} {--queue= : Named queue} {--json}';

    protected $description = 'Show fair queue depth, running jobs, and counters';

    public function handle(QueueFactory $queues): int
    {
        $connection = $queues->connection((string) $this->option('connection'));

        if (! $connection instanceof FairRedisQueue) {
            $this->error('The selected connection is not a fair-redis queue.');

            return self::FAILURE;
        }

        $snapshot = $connection->snapshot($this->option('queue'));

        if ($this->option('json')) {
            $this->line(json_encode($snapshot, JSON_THROW_ON_ERROR));
        } else {
            $this->table(['Metric', 'Value'], [
                ['Ready', $snapshot['ready']],
                ['Delayed', $snapshot['delayed']],
                ['Reserved', $snapshot['reserved']],
                ['Running', $snapshot['running']],
            ]);
            $this->line('Per-partition ready depth: '.json_encode($snapshot['depths'], JSON_THROW_ON_ERROR));
            $this->line('Counters: '.json_encode($snapshot['metrics'], JSON_THROW_ON_ERROR));
        }

        return self::SUCCESS;
    }
}
