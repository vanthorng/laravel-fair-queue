<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Tests\Integration;

use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Teerex\FairQueue\FairRedisQueue;
use Teerex\FairQueue\Tests\Support\ProbeJob;
use Teerex\FairQueue\Tests\TestCase;

final class ConcurrencyTest extends TestCase
{
    public function test_global_slot_is_atomic_across_two_processes(): void
    {
        config()->set('fair-queue.max_concurrent', 1);
        $queue = $this->app->make(QueueFactory::class)->connection('fair');
        self::assertInstanceOf(FairRedisQueue::class, $queue);
        $queue->push(new ProbeJob('a'));
        $queue->push(new ProbeJob('b'));

        $script = dirname(__DIR__).'/Support/reserve-child.php';
        $arguments = [
            PHP_BINARY,
            $script,
            $queue->keys()->base,
            (string) (getenv('REDIS_HOST') ?: '127.0.0.1'),
            (string) (getenv('REDIS_PORT') ?: '6379'),
            '15',
            (string) config('database.redis.options.prefix', ''),
        ];
        $processes = [];

        for ($i = 0; $i < 2; $i++) {
            $process = proc_open(
                $arguments,
                [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
                $pipes,
                dirname(__DIR__, 2),
                null,
                ['bypass_shell' => true],
            );
            self::assertIsResource($process);
            fclose($pipes[0]);
            $processes[] = [$process, $pipes];
        }

        $claimed = [];
        foreach ($processes as [$process, $pipes]) {
            $stdout = trim((string) stream_get_contents($pipes[1]));
            $stderr = trim((string) stream_get_contents($pipes[2]));
            fclose($pipes[1]);
            fclose($pipes[2]);
            self::assertSame(0, proc_close($process), $stderr);
            if ($stdout !== 'none') {
                $claimed[] = $stdout;
            }
        }

        self::assertCount(1, $claimed);
        self::assertSame(1, $queue->reservedSize());
    }
}
