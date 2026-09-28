<?php

declare(strict_types=1);

return [
    /*
    |--------------------------------------------------------------------------
    | Monitoring switch
    |--------------------------------------------------------------------------
    |
    | Off until you turn it on, so installing the package costs nothing before
    | a counter store is chosen. When false no queue events are observed and
    | nothing is recorded.
    |
    */

    'enabled' => env('QUEUE_MONITOR_ENABLED', false),

    /*
    |--------------------------------------------------------------------------
    | Monitored queues
    |--------------------------------------------------------------------------
    |
    | The queues to count and sample, keyed by connection name. Throughput is
    | counted only for these pairs, because a counter nobody samples would cost
    | a cache write per job and never be published. An empty map monitors the
    | default queue of the default connection. A failover connection is
    | monitored as its first connection, the one it pushes to and pops from,
    | and a background connection as sync, which runs its jobs.
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
    | Where samples are delivered: 'emf', or any driver registered with
    | MetricSinkManager::extend().
    |
    */

    'sink' => env('QUEUE_MONITOR_SINK', 'emf'),

    /*
    |--------------------------------------------------------------------------
    | CloudWatch Embedded Metric Format
    |--------------------------------------------------------------------------
    |
    | Writes one JSON line per dimension tuple to stdout. Nothing is sent to
    | AWS from here and no SDK is involved: the platform's log driver ships the
    | line to CloudWatch Logs, which extracts the metrics.
    |
    | The scheduler sends a command's output to /dev/null unless told
    | otherwise, so schedule the sampler with ->appendOutputTo(...); the
    | sampler refuses to drain the counters into /dev/null.
    |
    */

    'emf' => [
        'namespace' => env('QUEUE_MONITOR_NAMESPACE', 'Laravel/Queue'),

        'entity' => array_filter([
            'Service' => env('QUEUE_MONITOR_SERVICE'),
            'Environment' => env('QUEUE_MONITOR_ENVIRONMENT'),
        ], filled(...)),
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
    | Use a redis store, ideally with a connection of its own and tight
    | timeouts. DynamoDB also works. Any other store is refused when first
    | used.
    |
    */

    'counters' => [
        'store' => env('QUEUE_MONITOR_CACHE_STORE'),

        'prefix' => env('QUEUE_MONITOR_CACHE_PREFIX', 'queue-monitor'),
    ],

    /*
    |--------------------------------------------------------------------------
    | Job class dimension
    |--------------------------------------------------------------------------
    |
    | JobsQueued, JobsCompleted and JobsFailed are also published per job
    | class. The first classes seen on a queue keep their own JobClass
    | dimension; the rest are folded into a single bucket, so the per-class
    | values still add up to the queue total and the set of published metrics
    | stays fixed. Every class adds CloudWatch custom metrics; 0 publishes
    | the queue totals only.
    |
    */

    'max_job_classes' => env('QUEUE_MONITOR_MAX_JOB_CLASSES', 25),
];
