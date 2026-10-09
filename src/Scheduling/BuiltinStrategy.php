<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Scheduling;

use Teerex\FairQueue\Contracts\SchedulingStrategy;

abstract readonly class BuiltinStrategy implements SchedulingStrategy
{
    public function reservationScript(): string
    {
        $script = file_get_contents(dirname(__DIR__, 2).'/resources/lua/reserve.lua');

        if ($script === false) {
            throw new \RuntimeException('Fair queue reservation script is missing.');
        }

        return $script;
    }
}
