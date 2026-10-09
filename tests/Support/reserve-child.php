<?php

declare(strict_types=1);

require dirname(__DIR__, 2).'/vendor/autoload.php';

use Predis\Client;
use Teerex\FairQueue\Redis\Keys;

$keys = new Keys($argv[1]);
$client = new Client([
    'scheme' => 'tcp',
    'host' => $argv[2],
    'port' => (int) $argv[3],
    'database' => (int) $argv[4],
], ['prefix' => $argv[5]]);
$script = file_get_contents(dirname(__DIR__, 2).'/resources/lua/reserve.lua');
$now = (int) floor(microtime(true) * 1000);
$redisKeys = [
    $keys->key('ring'), $keys->key('members'), $keys->key('wfq'),
    $keys->key('jobs'), $keys->key('partitions'), $keys->key('reserved'),
    $keys->key('leases'), $keys->key('running'), $keys->key('global-running'),
    $keys->key('depths'), $keys->key('metrics'), $keys->key('process-next'),
    $keys->key('deficit'), $keys->key('weights'), $keys->key('penalties'), $keys->key('starts'),
    $keys->key('wfq-cursor'), $keys->key('max-concurrent'),
    $keys->key('process-rates'), $keys->key('process-bursts'),
    $keys->key('wait-ms'), $keys->key('reserved-count'), $keys->key('delayed'),
];
$result = $client->eval(...array_merge(
    [$script, count($redisKeys)],
    $redisKeys,
    [$keys->readyPrefix(), 'drr', $now, $now + 5000, 1, 100, 128, 0, 1, bin2hex(random_bytes(16)), 20],
));

echo is_array($result) ? $result[2] : 'none';
