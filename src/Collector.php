<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Queue\Factory;
use Illuminate\Foundation\Cloud\FailedJobProvider as CloudFailedJobProvider;
use Illuminate\Queue\Failed\CountableFailedJobProvider;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Queue\Failed\NullFailedJobProvider;
use Illuminate\Queue\SyncQueue;
use ReflectionProperty;
use Throwable;

final readonly class Collector {
    public function __construct(
        private Factory $queues,
        private Counters $counters,
        private FailedJobProviderInterface $failer,
        private ExceptionHandler $handler,
        private QueueMonitor $monitor,
    ) {}

    /**
     * @return array{0: list<Metric>, 1: array<string, array<string, int>>} the metrics, and the readings to commit once they are published
     */
    public function collect(string $connection, string $queue): array {
        $dimensions = ['Connection' => $connection, 'Queue' => $queue];
        $driver = $this->queues->connection($connection);

        $metrics = [
            new Metric(MetricName::JobsPending, $driver->pendingSize($queue), $dimensions),
            new Metric(MetricName::JobsDelayed, $driver->delayedSize($queue), $dimensions),
            new Metric(MetricName::JobsInProgress, $driver->reservedSize($queue), $dimensions),
        ];

        // Database and beanstalkd jobs record a forwarded queue's target, Redis
        // jobs the name their worker was started with, so count both.
        $failed = $this->failedJobs($connection, array_values(array_unique([$queue, $this->monitor->physicalQueue($connection, $queue)])));

        if ($failed !== null) {
            $metrics[] = new Metric(MetricName::FailedJobsTotal, $failed, $dimensions);
        }

        // Counters exist only for monitored queues.
        if (! in_array([$connection, $queue], $this->monitor->sampledQueues(), true)) {
            return [$metrics, []];
        }

        $classes = $this->counters->classes($connection, $queue);
        $readings = $this->counters->read($connection, $queue, $classes);
        $max = $this->monitor->maxJobClasses();
        $keep = array_flip(array_slice(array_values(array_diff($classes, [Counters::OTHER])), 0, $max));

        foreach (MetricName::COUNTERS as $metric) {
            // Sync connections never fire JobQueued.
            if ($metric === MetricName::JobsQueued && $driver instanceof SyncQueue) {
                continue;
            }

            $counts = $readings[$metric] ?? [];
            $metrics[] = new Metric($metric, array_sum($counts), $dimensions);

            foreach ($max > 0 ? $this->fold($counts, $keep) : [] as $class => $value) {
                $metrics[] = new Metric($metric, $value, [...$dimensions, 'JobClass' => (string) $class]);
            }
        }

        return [$metrics, $readings];
    }

    /**
     * @param array<array-key, int> $counts
     * @param array<string, int>    $keep
     *
     * @return array<string, int>
     */
    private function fold(array $counts, array $keep): array {
        $folded = [];

        foreach ($counts as $class => $value) {
            $label = isset($keep[$class]) ? (string) $class : Counters::OTHER;
            $folded[$label] = ($folded[$label] ?? 0) + $value;
        }

        return $folded;
    }

    /**
     * Null when the provider cannot count, fails, or only ever answers zero,
     * as Cloud's does for its managed queues, whose failures it never stores.
     *
     * @param list<string> $queues
     */
    private function failedJobs(string $connection, array $queues): ?int {
        try {
            // Cloud wraps the application's provider and answers zero for one
            // that cannot count, so judge the provider it wraps.
            $failer = $this->failer instanceof CloudFailedJobProvider
                ? (new ReflectionProperty(CloudFailedJobProvider::class, 'failer'))->getValue($this->failer)
                : $this->failer;

            if (
                ! $failer instanceof CountableFailedJobProvider
                || $failer instanceof NullFailedJobProvider
                || ($this->failer instanceof CloudFailedJobProvider && $connection === 'cloud')
            ) {
                return null;
            }

            return array_sum(array_map(fn (string $queue): int => (int) $failer->count($connection, $queue), $queues));
        } catch (Throwable $e) {
            $this->handler->report($e);

            return null;
        }
    }
}
