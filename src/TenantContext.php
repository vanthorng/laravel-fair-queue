<?php

declare(strict_types=1);

namespace Teerex\FairQueue;

final class TenantContext
{
    private ?Partition $partition = null;

    public function current(): ?Partition
    {
        return $this->partition;
    }

    public function enter(Partition $partition): void
    {
        $this->partition = $partition;
    }

    public function clear(): void
    {
        $this->partition = null;
    }

    public function runFor(Partition $partition, callable $callback): mixed
    {
        $previous = $this->partition;
        $this->partition = $partition;

        try {
            return $callback();
        } finally {
            $this->partition = $previous;
        }
    }
}
