<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

final class MetricName {
    public const string JobsQueued = 'JobsQueued';

    public const string JobsCompleted = 'JobsCompleted';

    public const string JobsFailed = 'JobsFailed';

    public const string JobsPending = 'JobsPending';

    public const string JobsDelayed = 'JobsDelayed';

    public const string JobsInProgress = 'JobsInProgress';

    public const string FailedJobsTotal = 'FailedJobsTotal';

    /**
     * Accumulated by the listener and drained by the sampler.
     */
    public const array COUNTERS = [self::JobsQueued, self::JobsCompleted, self::JobsFailed];
}
