<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Contracts;

interface SchedulingStrategy
{
    /** Redis script mode; built-ins use rr, drr, or wfq. */
    public function mode(): string;

    /** Complete, bounded atomic reservation script obeying the fair queue return contract. */
    public function reservationScript(): string;
}
