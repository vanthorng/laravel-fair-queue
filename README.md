# Laravel Fair Queue

`teerex/laravel-fair-queue` adds a dedicated Redis queue connection that shares worker capacity among tenant or user-group partitions. It supports round robin, deficit round robin, and weighted fair queuing, partition limits, rate limits, and noisy-neighbor penalties. Jobs use Laravel's normal `queue:work` execution path.

## Status and compatibility

This package targets PHP 8.3+ and Laravel 11, 12, and 13. On local Windows/PHP 8.3, the full suite passed against Laravel 11.57.0, 12.69.3, and 13.35.0 (19 tests and 1,169 assertions per version). Composer validation and Pint passed; PHPStan level 6 passed against Laravel 13. Linux, PHP 8.4, deployment failover, and sustained load remain CI and operational release gates; do not treat these local results alone as production certification.

Composer audit reported four advisories against the latest Laravel 11 framework used in the compatibility run. Applications choosing Laravel 11 must evaluate those upstream advisories and their own exposure. Prefer a patched Laravel 12 or 13 release for new deployments, and run `composer audit` against the host application's resolved dependency set.

Use a standalone Redis primary or Redis Sentinel deployment. Redis Cluster is not supported: the current Lua scripts derive per-partition ready keys at runtime. Use a dedicated Redis database or key prefix and `maxmemory-policy noeviction`. Configure persistence and backup according to your durability needs.

## Install

Until the package is published to Packagist, add this repository as a Composer `path` or `vcs` repository in the host application. The package name and discovery metadata are ready for Composer installation.

```bash
composer require teerex/laravel-fair-queue
php artisan vendor:publish --tag=fair-queue-config
```

After publishing, add or override a queue connection in the host application's `config/queue.php`:

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

The service provider creates this connection with defaults when the application has not defined it. Composer package discovery loads the provider automatically.

Bind a resolver when tenant identity already lives in your application context:

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

Alternatively, implement `PartitionedJob` on a job or wrap existing dispatches in `TenantContext::runFor()`:

```php
use Teerex\FairQueue\Partition;
use Teerex\FairQueue\TenantContext;
use Illuminate\Support\Facades\Queue;

app(TenantContext::class)->runFor(
    new Partition('tenant', 'acme'),
    fn () => Queue::connection('fair')->push(new ProcessReport($report)),
);
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
