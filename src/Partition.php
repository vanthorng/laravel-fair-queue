<?php

declare(strict_types=1);

namespace Teerex\FairQueue;

use InvalidArgumentException;

final readonly class Partition
{
    public function __construct(public string $kind, public string $id)
    {
        if (! in_array($kind, ['tenant', 'group'], true) || $id === '' || strlen($id) > 200) {
            throw new InvalidArgumentException('Fair queue partition must be a tenant or group with a non-empty ID of at most 200 bytes.');
        }
    }

    public function key(): string
    {
        return $this->kind.':'.bin2hex($this->id);
    }
}
