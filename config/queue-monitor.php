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

    /*
    |--------------------------------------------------------------------------
    | Sampled queues
    |--------------------------------------------------------------------------
    |
    | Depth is pulled rather than pushed, so the sampler has to be told which
    | queues to read: no driver exposes a cheap way to enumerate them. Keys are
    | connection names, values the queues on that connection.
    |
    |     'queues' => ['redis' => ['default', 'high']],
    |
    */

    'queues' => [],

    /*
    |--------------------------------------------------------------------------
    | Metric sink
    |--------------------------------------------------------------------------
    |
    | Where samples are delivered: 'emf', 'null', or any driver registered with
    | MetricSinkManager::extend().
    |
    */

    'sink' => env('QUEUE_MONITOR_SINK', 'null'),

    /*
    |--------------------------------------------------------------------------
    | CloudWatch Embedded Metric Format
    |--------------------------------------------------------------------------
    |
    | Writes one JSON line per dimension tuple. Nothing is sent to AWS from here
    | and no SDK is involved: the platform's log driver ships the line to
    | CloudWatch Logs, which extracts the metrics.
    |
    | Leaving `channel` null writes to php://stdout. Setting it routes through a
    | Laravel log channel, which MUST emit the raw message: the default
    | formatter's "[2026-01-01 00:00:00] production.INFO:" prefix makes the line
    | unparseable and the metrics silently disappear.
    |
    | CloudWatch bills per custom metric, and a custom metric is one metric name
    | paired with one dimension tuple. `max_job_classes` is what bounds that.
    |
    */

    'emf' => [
        'namespace' => env('QUEUE_MONITOR_NAMESPACE', 'Laravel/Queue'),

        'channel' => env('QUEUE_MONITOR_EMF_CHANNEL'),

        'entity' => array_filter([
            'Service' => env('QUEUE_MONITOR_SERVICE'),
            'Environment' => env('QUEUE_MONITOR_ENVIRONMENT'),
        ], is_string(...)),
    ],

    /*
    |--------------------------------------------------------------------------
    | Counters
    |--------------------------------------------------------------------------
    |
    | Throughput is counted as the event fires, straight into a shared cache
    | store, so nothing accumulates in PHP memory and a worker killed mid-job
    | loses nothing but the job it was running.
    |
    | The store must increment atomically across processes. `array` is a
    | per-process buffer and `file` is not atomic under concurrency; both are
    | rejected at boot. Null uses the application's default store.
    |
    */

    'counters' => [
        'store' => env('QUEUE_MONITOR_CACHE_STORE'),

        'prefix' => 'queue-monitor',
    ],

    /*
    |--------------------------------------------------------------------------
    | Job class cardinality
    |--------------------------------------------------------------------------
    |
    | The busiest job classes keep their own JobClass dimension; the rest are
    | folded into a single bucket so the per-class values still add up to the
    | queue total.
    |
    */

    'max_job_classes' => 25,
];
