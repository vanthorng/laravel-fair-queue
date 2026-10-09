<?php

declare(strict_types=1);

namespace Teerex\FairQueue\Exceptions;

use RuntimeException;

final class RedisUnavailable extends RuntimeException {}
