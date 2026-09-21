<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobQueued;
use Throwable;

final class RecordJobMetrics {
    private bool $reported = false;

    public function __construct(
        private readonly QueueMonitor $monitor,
        private readonly Counters $counters,
        private readonly Repository $config,
        private readonly ExceptionHandler $handler,
    ) {}

    public function handleJobQueued(JobQueued $event): void {
        $this->guard(function () use ($event): void {
            $connection = (string) $event->connectionName;

            if (! $this->monitor->monitors($connection)) {
                return;
            }

            /** @var mixed $name */
            $name = $event->payload()['displayName'] ?? null;

            $this->counters->increment(
                $connection,
                $event->queue ?? $this->defaultQueue($connection),
                MetricName::JobsQueued,
                is_string($name) ? $name : 'unknown',
            );
        });
    }

    public function handleJobProcessed(JobProcessed $event): void {
        $this->record($event->connectionName, $event->job->getQueue(), $event->job->resolveName(), MetricName::JobsCompleted);
    }

    public function handleJobFailed(JobFailed $event): void {
        $this->record($event->connectionName, $event->job->getQueue(), $event->job->resolveName(), MetricName::JobsFailed);
    }

    private function record(string $connection, string $queue, string $class, string $metric): void {
        $this->guard(function () use ($connection, $queue, $class, $metric): void {
            if ($this->monitor->monitors($connection)) {
                $this->counters->increment($connection, $queue, $metric, $class);
            }
        });
    }

    private function defaultQueue(string $connection): string {
        $queue = $this->config->get("queue.connections.{$connection}.queue");

        return is_string($queue) && $queue !== '' ? $queue : 'default';
    }

    /**
     * A metrics failure must never reach the job. Report the first one per
     * process, then stay quiet.
     */
    private function guard(callable $callback): void {
        try {
            $callback();
        } catch (Throwable $e) {
            if ($this->reported) {
                return;
            }

            $this->reported = true;

            try {
                $this->handler->report($e);
            } catch (Throwable) {
                // A broken reporter must not break the job either.
            }
        }
    }
}
