<?php

declare(strict_types=1);

use Illuminate\Support\Facades\Route;
use Teerex\FairQueue\Http\MetricsController;

Route::middleware((array) config('fair-queue.metrics.middleware', ['auth']))
    ->prefix((string) config('fair-queue.metrics.route_prefix', 'fair-queue'))
    ->group(function (): void {
        Route::get('/metrics', [MetricsController::class, 'json']);
        if ((bool) config('fair-queue.metrics.prometheus', false)) {
            Route::get('/metrics/prometheus', [MetricsController::class, 'prometheus']);
        }
    });
