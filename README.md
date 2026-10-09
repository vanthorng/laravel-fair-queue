# Laravel Fair Queue

`teerex/laravel-fair-queue` shares Redis queue worker capacity across tenants or user groups. A large backlog in one partition does not have to delay every other partition. Jobs still run through Laravel's standard `queue:work` command.

Choose round robin, deficit round robin, or weighted fair queuing. Optional concurrency limits, rate limits, and noisy-neighbor controls provide further isolation.

## Quick start

The examples below assume the package is installed in your Laravel application. See [Installation](#installation) for the current local-repository setup.

1. Publish the configuration:

   ```bash
   php artisan vendor:publish --tag=fair-queue-config
   ```

2. Dispatch a job in a partition. Replace `ProcessReport` and `$report` with a job and data from your application:

   ```php
   use Illuminate\Support\Facades\Queue;
   use Teerex\FairQueue\Partition;
   use Teerex\FairQueue\TenantContext;

   app(TenantContext::class)->runFor(
       new Partition('tenant', 'acme'),
       fn () => Queue::connection('fair')->push(new ProcessReport($report)),
   );
   ```

3. Start a dedicated worker:

   ```bash
   php artisan queue:work fair --queue=default --timeout=90 --sleep=1 --tries=3
   ```

The service provider supplies a `fair` connection when the application has not defined one. Each job needs one partition; dispatch without an identity is rejected. Set `retry_after` higher than the worker timeout and the longest expected job duration, with room for shutdown. See [Partition assignment](#partition-assignment) and [Configuration](#configuration) for other options.

## Why use it?

On a shared queue, a tenant that dispatches a large backlog can occupy every available worker. Smaller tenants then wait behind that backlog even when their jobs are time-sensitive. This package partitions jobs by tenant or user group and schedules active partitions fairly, so a busy partition cannot monopolize the queue.

It also enforces queue-wide and per-partition concurrency limits, applies dispatch and processing rate limits, and reduces capacity allocated to partitions that consistently create excessive backlog, slow jobs, or failures. Healthy partitions continue receiving service while a noisy neighbor is throttled.

## How it works

Jobs sent through the `fair-redis` connection are assigned one partition key by a configurable resolver. Redis stores ready jobs per partition, tracks delayed and reserved jobs, and maintains the scheduler state for the named fair queue.

When a Laravel worker calls `queue:work`, the scheduler selects an eligible partition and reserves one job. Reservation and concurrency accounting happen atomically in Redis. The package tracks delayed and reserved jobs and recovers expired reservations.

Workers promote delayed jobs and recover expired reservations opportunistically. Laravel's normal retry, backoff, timeout, and failed-job flow still applies. Delivery is at least once, so job handlers must be idempotent. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the scheduling and lease contracts.

### Choose a scheduling strategy

| Strategy | Setting | Use when |
| --- | --- | --- |
| Round robin | `rr` | Each active partition should take turns. |
| Deficit round robin (default) | `drr` | You want weighted shares while preserving service for active partitions. |
| Weighted fair queuing | `wfq` | You want weighted scheduling based on virtual service order. |

Set `strategy` in `config/fair-queue.php`. The default is `drr`; see [Configuration](#configuration) for weights and limits.

## Requirements

This package targets PHP 8.3+ and Laravel 11, 12, and 13. It requires a standalone Redis primary or Redis Sentinel deployment. Redis Cluster is not supported because the Lua scripts derive per-partition ready keys at runtime. Use a dedicated Redis database or key prefix and `maxmemory-policy noeviction`. Configure persistence and backup according to your durability needs.

See [compatibility and verification notes](docs/COMPATIBILITY.md) for the tested version matrix, outstanding release gates, and Laravel 11 advisory details. Run `composer audit` against your application's resolved dependencies.

## Installation

Until the package is published to Packagist, add it as a Composer `path` repository in the host application's `composer.json`. Adjust `../laravel-fair-queue` to the package directory relative to that file:

```json
{
  "repositories": [
    {
      "type": "path",
      "url": "../laravel-fair-queue"
    }
  ]
}
```

Then install and publish the configuration:

```bash
composer require "teerex/laravel-fair-queue:*@dev"
php artisan vendor:publish --tag=fair-queue-config
```

To override the provider's default connection settings, add a `fair` connection in the host application's `config/queue.php`:

```php
'connections' => [
    'fair' => [
        'driver' => 'fair-redis',
        'connection' => 'default',
        'queue' => 'default',
        'retry_after' => 120,
        'after_commit' => true,
    ],
],
```

Composer package discovery loads the provider automatically. It creates a `fair` connection with defaults when the application has not defined one. Define the connection above only when you need to override those defaults.

## Partition assignment

The [quick start](#quick-start) uses `TenantContext::runFor()` to assign a partition around an existing dispatch. You can instead implement `PartitionedJob` on the job or bind a resolver when tenant identity already lives in your application context:

```php
use Teerex\FairQueue\Contracts\PartitionResolver;
use Teerex\FairQueue\Partition;

$this->app->bind(PartitionResolver::class, function ($app) {
    return new class($app) implements PartitionResolver {
        public function __construct(private $app) {}

        public function resolve(mixed $job, mixed $data = null): ?Partition
        {
            $tenantId = $this->app->make(CurrentTenant::class)->id();
            return $tenantId === null ? null : new Partition('tenant', (string) $tenantId);
        }
    };
});
```

Choose `new Partition('group', 'staff')` for user-group fairness. Each job has one partition key. Missing identity rejects dispatch. The queue payload carries the partition so retries and worker-side child dispatches retain context. `RestorePartition` is an optional job middleware for classes that implement `PartitionedJob`; the provider also restores context from the payload around fair job execution.

Run dedicated fair workers:

```bash
php artisan queue:work fair --queue=default --timeout=90 --sleep=1 --tries=3
```

Set `retry_after` **higher** than the longest job or worker timeout, with margin for shutdown. All fair workers for a logical queue must use this package connection. Do not point native Redis queue workers at package-managed keys.

## Configuration

Publish `config/fair-queue.php` for defaults. `strategy` accepts `rr`, `drr` (default), `wfq`, or a class implementing `SchedulingStrategy`. Weights are bounded by `weights.maximum`. A per-partition configuration entry uses the partition's canonical key:

```php
use Teerex\FairQueue\Partition;

'partitions' => [
    (new Partition('tenant', 'acme'))->key() => [
        'weight' => 3,
        'priority' => 1,
        'max_concurrent' => 5,
        'dispatch_rate' => 20,
        'dispatch_burst' => 40,
        'processing_rate' => 10,
        'processing_burst' => 20,
    ],
],
```

`max_concurrent` caps simultaneous jobs **per logical queue**; `tenant_max_concurrent` provides the default partition cap. A zero rate disables that rate limit. Dispatch over a rate limit is accepted into delayed state; processing over a rate limit leaves jobs ready. Idle tenants do not consume slots. Noisy-neighbor thresholds reduce effective weight and processing rate to `penalty_percent`, with a floor that still permits service. Recovery requires the configured number of healthy windows.

The dispatcher accepts an optional custom `SchedulingStrategy::reservationScript()`. Such a script must preserve the key, argument, return, lease, and atomicity contract documented in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Custom scripts should be load-tested and reviewed before use.

## Operations and metrics

```bash
php artisan fair-queue:doctor
php artisan fair-queue:status --json
php artisan fair-queue:maintain
```

Workers promote delayed jobs and reclaim expired leases opportunistically. Schedule `fair-queue:maintain` every minute if queues can remain idle or workers are not continuously running. The optional `/fair-queue/metrics` JSON route is disabled by default and uses the configured authentication middleware. `/fair-queue/metrics/prometheus` is also opt-in. Avoid exposing per-tenant metrics on an unauthenticated route or enabling tenant labels with high-cardinality IDs.

The package emits `NeighborStateChanged` events and structured logs when throttling changes. Laravel's regular queue events remain available. `queue:clear` is deliberately unsupported for this connection because clearing a live queue could remove reserved jobs and break slot accounting. For planned deletion, stop producers and workers, wait for reservations to finish or expire, take a backup, and delete only the keys for that logical queue prefix during a maintenance window.

## Delivery and failure model

Delivery is **at least once**, conditional on Redis retaining acknowledged writes. Enqueue, promotion, reservation, acknowledgement, release, and expiry recovery are coordinated by atomic Redis scripts. A reservation token prevents a stale worker from acknowledging or releasing a newer reservation. A process that pauses beyond its lease can still resume while a retry runs; exactly-once side effects and absolute non-overlap cannot be guaranteed without idempotency or fencing in the downstream system.

Redis outages fail closed: dispatch throws if enqueue cannot be confirmed and workers cannot reserve. If acknowledgement fails after a job's side effect, the job can be retried when its lease expires. Redis replication and AOF settings determine how much acknowledged data can be lost during failover. Configure retryable, idempotent jobs and monitor Redis durability and memory.

Redis Lua scripts are atomic to other clients, but a script error does not roll back earlier writes. Use `noeviction`, reserve memory headroom, monitor Redis error and memory metrics, and rehearse recovery from interrupted operations before production rollout.

Horizon can continue supervising ordinary Redis queues, but it does not supervise this custom connection or accurately display its tenant-level scheduling. Run fair workers under Supervisor, systemd, Kubernetes, or another process manager and use the package metrics for them.

## Development checks

With PHP 8.3+, Composer, and Redis available:

```bash
composer install
composer validate --strict
vendor/bin/pest
vendor/bin/pint --test
vendor/bin/phpstan analyse src --level=6
```

Run the test suite against Laravel 11, 12, and 13, on PHP 8.3 and 8.4. The Redis integration suite uses Redis database 15 by default and generates a unique prefix per test. The multi-process test starts independent PHP processes on Windows and Linux. See the CI workflow for the version matrix.

## Limitations

- Redis Cluster and Horizon-managed fair workers are unsupported.
- The queue's global concurrency cap covers one named fair queue, not every queue in an application.
- A delayed job's availability time is approximate to worker polling and maintenance cadence.
- The package does not change external job side effects; job handlers remain responsible for idempotency.
- `queue:clear` is unsupported on a live fair queue.
