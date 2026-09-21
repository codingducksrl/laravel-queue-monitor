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
     * The counters accumulated by the listener and drained by the sampler.
     *
     * @return list<string>
     */
    public static function counters(): array {
        return [self::JobsQueued, self::JobsCompleted, self::JobsFailed];
    }
}
