<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Commands;

use Illuminate\Console\Command;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Teerex\FairQueue\FairRedisQueue;
use Throwable;

final class DoctorCommand extends Command
{
    protected $signature = 'fair-queue:doctor {--connection=fair}';

    protected $description = 'Check the fair queue connection and lease settings';

    public function handle(QueueFactory $queues): int
    {
        try {
            $queue = $queues->connection((string) $this->option('connection'));

            if (! $queue instanceof FairRedisQueue) {
                $this->error('The selected connection is not a fair-redis queue.');

                return self::FAILURE;
            }

            $pong = $queue->getConnection()->ping();
            $this->info('Redis reachable: '.(string) $pong);

            $retryAfter = (int) config('queue.connections.'.$this->option('connection').'.retry_after', 120);
            if ($retryAfter <= 0) {
                $this->error('retry_after must be positive.');

                return self::FAILURE;
            }

            $this->info("Lease expires after {$retryAfter} seconds. Configure worker --timeout below this value.");

            return self::SUCCESS;
        } catch (Throwable $exception) {
            $this->error('Fair queue health check failed: '.$exception->getMessage());

            return self::FAILURE;
        }
    }
}
