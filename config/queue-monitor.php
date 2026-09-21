<?php

declare(strict_types=1);

return [
    /*
    |--------------------------------------------------------------------------
    | Monitoring switch
    |--------------------------------------------------------------------------
    |
    | Turns the package on or off wholesale. When false, no queue events are
    | observed and nothing is recorded, which is the cheapest way to take the
    | package out of a hot path without uninstalling it.
    |
    */

    'enabled' => env('QUEUE_MONITOR_ENABLED', true),

    /*
    |--------------------------------------------------------------------------
    | Monitored connections
    |--------------------------------------------------------------------------
    |
    | The queue connections to monitor, by name as they appear in the
    | application's `queue.connections` config. An empty list monitors every
    | connection.
    |
    */

    'connections' => [],
];
