<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Commands;

use Illuminate\Console\Command;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Teerex\FairQueue\FairRedisQueue;

final class MaintainCommand extends Command
{
    protected $signature = 'fair-queue:maintain {--connection=fair} {--queue= : Named queue}';

    protected $description = 'Promote delayed jobs and reclaim expired reservations';

    public function handle(QueueFactory $queues): int
    {
        $connection = $queues->connection((string) $this->option('connection'));

        if (! $connection instanceof FairRedisQueue) {
            $this->error('The selected connection is not a fair-redis queue.');

            return self::FAILURE;
        }

        $result = $connection->maintain($this->option('queue'));
        $this->info("Promoted {$result['promoted']} jobs; reclaimed {$result['reclaimed']} expired reservations.");

        return self::SUCCESS;
    }
}
