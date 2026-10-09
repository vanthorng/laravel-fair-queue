<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Http;

use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Response;
use Teerex\FairQueue\FairRedisQueue;

final readonly class MetricsController
{
    public function __construct(private QueueFactory $queues) {}

    public function json(): JsonResponse
    {
        return new JsonResponse($this->snapshot());
    }

    public function prometheus(): Response
    {
        $snapshot = $this->snapshot();
        $lines = [];

        foreach (['ready', 'delayed', 'reserved', 'running'] as $name) {
            $lines[] = "# TYPE fair_queue_{$name} gauge";
            $lines[] = "fair_queue_{$name} ".(int) $snapshot[$name];
        }

        foreach ($snapshot['metrics'] as $name => $value) {
            if (preg_match('/^[a-z][a-z0-9_]*$/', (string) $name)) {
                $lines[] = "fair_queue_{$name}_total ".(float) $value;
            }
        }

        if ((bool) config('fair-queue.metrics.tenant_labels', false)) {
            foreach ([
                'depths' => 'depth',
                'running_by_partition' => 'running_partition',
                'failures_by_partition' => 'failures_partition_total',
                'completed_recent_by_partition' => 'completed_recent',
                'average_wait_ms_by_partition' => 'average_wait_ms',
            ] as $source => $metric) {
                foreach ($snapshot[$source] as $partition => $value) {
                    $label = str_replace(['\\', '"', "\n"], ['\\\\', '\\"', '\\n'], (string) $partition);
                    $lines[] = "fair_queue_{$metric}{partition=\"{$label}\"} ".(float) $value;
                }
            }
        }

        return new Response(implode("\n", $lines)."\n", 200, ['Content-Type' => 'text/plain; version=0.0.4']);
    }

    /** @return array<string, mixed> */
    private function snapshot(): array
    {
        $queue = $this->queues->connection('fair');

        if (! $queue instanceof FairRedisQueue) {
            throw new \RuntimeException('The fair queue connection is not configured.');
        }

        return $queue->snapshot();
    }
}
