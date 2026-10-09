<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Redis;

final readonly class Keys
{
    public function __construct(public string $base) {}

    public function key(string $suffix): string
    {
        return $this->base.':'.$suffix;
    }

    public function ready(string $partition): string
    {
        return $this->key('ready:'.$partition);
    }

    public function readyPrefix(): string
    {
        return $this->key('ready:');
    }
}
