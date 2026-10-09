<?php

declare(strict_types=1);

return [
    'redis_connection' => env('FAIR_QUEUE_REDIS_CONNECTION', 'default'),
    'queue' => env('FAIR_QUEUE_NAME', 'default'),
    'prefix' => env('FAIR_QUEUE_PREFIX', 'fairq'),
    'strategy' => env('FAIR_QUEUE_STRATEGY', 'drr'), // rr, drr, wfq
    'retry_after' => (int) env('FAIR_QUEUE_RETRY_AFTER', 120),
    'max_concurrent' => (int) env('FAIR_QUEUE_MAX_CONCURRENT', 100),
    'tenant_max_concurrent' => (int) env('FAIR_QUEUE_TENANT_MAX_CONCURRENT', 10),
    'max_scan' => (int) env('FAIR_QUEUE_MAX_SCAN', 128),
    'maintenance_batch' => (int) env('FAIR_QUEUE_MAINTENANCE_BATCH', 100),
    'maintenance_interval_ms' => (int) env('FAIR_QUEUE_MAINTENANCE_INTERVAL_MS', 250),
    'weights' => [
        'default' => 1,
        'maximum' => 20,
    ],
    'partitions' => [
        // (new Partition('tenant', 'acme'))->key() =>
        // ['weight' => 2, 'priority' => 0, 'max_concurrent' => 5,
        //  'dispatch_rate' => 10, 'dispatch_burst' => 20,
        //  'processing_rate' => 5, 'processing_burst' => 10],
    ],
    'dispatch_rate' => [
        'per_second' => 0, // 0 disables the limit.
        'burst' => 0,
    ],
    'processing_rate' => [
        'per_second' => 0,
        'burst' => 0,
    ],
    'noisy_neighbor' => [
        'enabled' => true,
        'window_seconds' => 60,
        'depth_threshold' => 10000,
        'failure_rate_threshold' => 0.25,
        'minimum_samples' => 20,
        'duration_ms_threshold' => 30000,
        'penalty_percent' => 50,
        'recovery_windows' => 3,
    ],
    'metrics' => [
        'enabled' => false,
        'route_prefix' => 'fair-queue',
        'middleware' => ['auth'],
        'prometheus' => false,
        'tenant_labels' => false,
    ],
];
