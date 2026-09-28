<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use Closure;
use Illuminate\Contracts\Container\Container;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\CallQueuedClosure;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobQueued;
use Illuminate\Support\InteractsWithTime;
use Throwable;

use function Illuminate\Support\enum_value;

final class RecordJobMetrics {
    use InteractsWithTime;

    /**
     * How long counting stands down after a failure, so an unreachable store
     * costs one timeout per process per minute instead of one per job.
     */
    private const int PAUSE = 60;

    private int $pausedUntil = 0;

    public function __construct(
        private readonly Container $app,
        private readonly QueueMonitor $monitor,
    ) {}

    public function handleJobQueued(JobQueued $event): void {
        $this->guard(function () use ($event): void {
            $connection = (string) $event->connectionName;
            $queue = enum_value($event->queue);

            $this->record(
                $connection,
                is_string($queue) && $queue !== '' ? $queue : $this->monitor->defaultQueue($connection),
                $this->displayName($event->job),
                MetricName::JobsQueued,
            );
        });
    }

    public function handleJobProcessed(JobProcessed $event): void {
        $this->guard(function () use ($event): void {
            // A job that released itself or called fail() returns normally too.
            if (! $event->job->isReleased() && ! $event->job->hasFailed()) {
                $this->recordJob($event->job, MetricName::JobsCompleted);
            }
        });
    }

    public function handleJobFailed(JobFailed $event): void {
        $this->guard(fn () => $this->recordJob($event->job, MetricName::JobsFailed));
    }

    /**
     * The job's own connection, not the worker's: a failover worker reports
     * the failover connection for jobs that were pushed to the one behind it.
     */
    private function recordJob(Job $job, string $metric): void {
        $payload = $job->payload();

        $this->record($job->getConnectionName(), (string) $job->getQueue(), self::label($payload['displayName'] ?? $payload['job'] ?? null), $metric);
    }

    private function record(string $connection, string $queue, string $class, string $metric): void {
        $queue = $this->monitor->monitoredQueue($connection, $queue);

        if ($queue !== null) {
            $this->app->make(Counters::class)->increment($connection, $queue, $metric, $class);
        }
    }

    /**
     * What Queue::createPayloadArray() stores as displayName, read from the
     * job itself rather than by decoding the whole payload.
     */
    private function displayName(mixed $job): string {
        return self::label(match (true) {
            $job instanceof Closure => CallQueuedClosure::create($job)->displayName(),
            is_object($job) => method_exists($job, 'displayName') ? $job->displayName() : $job::class,
            is_string($job) => explode('@', $job)[0],
            default => null,
        });
    }

    /**
     * Names are stored in the registry the sampler reads, so they are capped.
     */
    private static function label(mixed $name): string {
        return is_string($name) && $name !== '' ? mb_substr($name, 0, 255) : 'unknown';
    }

    /**
     * A metrics failure must never reach the job or the request. Counters is
     * resolved in here, so a misconfigured store is reported rather than
     * thrown into dispatch().
     */
    private function guard(Closure $callback): void {
        if ($this->pausedUntil > $this->currentTime()) {
            return;
        }

        try {
            $callback();
        } catch (Throwable $e) {
            $this->pausedUntil = $this->currentTime() + self::PAUSE;

            try {
                $this->app->make(ExceptionHandler::class)->report($e);
            } catch (Throwable) {
                // A broken reporter must not break the job either.
            }
        }
    }
}
