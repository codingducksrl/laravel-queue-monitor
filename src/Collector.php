<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Queue\Factory;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Foundation\Cloud\FailedJobProvider as CloudFailedJobProvider;
use Illuminate\Queue\DatabaseQueue;
use Illuminate\Queue\Failed\CountableFailedJobProvider;
use Illuminate\Queue\Failed\FailedJobProviderInterface;
use Illuminate\Queue\Failed\NullFailedJobProvider;
use Illuminate\Queue\Jobs\InspectedJob;
use Illuminate\Queue\SyncQueue;
use Illuminate\Support\Collection;
use Throwable;

final readonly class Collector {
    public const string OTHER = '__other__';

    public function __construct(
        private Factory $queues,
        private Counters $counters,
        private FailedJobProviderInterface $failer,
        private ExceptionHandler $handler,
        private QueueMonitor $monitor,
        private int $maxClasses = 25,
    ) {}

    /**
     * Returns the metrics to publish and the counter readings they were built
     * from. The caller commits the readings only once the sink has accepted
     * them, so a failed write loses nothing.
     *
     * @return array{0: list<Metric>, 1: array<string, array<string, int>>}
     */
    public function collect(string $connection, string $queue): array {
        $dimensions = ['Connection' => $connection, 'Queue' => $queue];
        $driver = $this->queues->connection($connection);

        // The database driver answers both with a scan of the jobs table, so
        // one scan serves both. Elsewhere the payloads are only fetched when
        // something is reserved.
        if ($driver instanceof DatabaseQueue) {
            $jobs = $this->reservedJobs($driver, $queue);
            $reserved = $jobs === null ? $driver->reservedSize($queue) : $jobs->count();
        } else {
            $reserved = $driver->reservedSize($queue);
            $jobs = $reserved > 0 ? $this->reservedJobs($driver, $queue) : null;
        }

        $metrics = [
            Metric::make(MetricName::JobsPending, $driver->pendingSize($queue), $dimensions),
            Metric::make(MetricName::JobsDelayed, $driver->delayedSize($queue), $dimensions),
            Metric::make(MetricName::JobsInProgress, $reserved, $dimensions),
        ];

        // Database and beanstalkd jobs record a forwarded queue's target, Redis
        // jobs the name their worker was started with, so count both.
        $failed = $this->failedJobs($connection, array_values(array_unique([$queue, $this->monitor->physicalQueue($connection, $queue)])));

        if ($failed !== null) {
            $metrics[] = Metric::make(MetricName::FailedJobsTotal, $failed, $dimensions);
        }

        $classes = $this->counters->classes($connection, $queue);
        $keep = array_flip(array_slice($classes, 0, $this->maxClasses));

        foreach ($this->fold($this->byClass($jobs), $keep) as $class => $count) {
            $metrics[] = Metric::make(MetricName::JobsInProgress, $count, [...$dimensions, 'JobClass' => (string) $class]);
        }

        // Only monitored queues are counted; a pair sampled from the command
        // line has depth but no throughput, and a zero would be a lie.
        if (! in_array([$connection, $queue], $this->monitor->sampledQueues(), true)) {
            return [$metrics, []];
        }

        $readings = $this->counters->read($connection, $queue, MetricName::counters(), $classes);

        // Totals are published even when zero: an idle window is a fact, and a
        // missing datapoint would leave "nothing completed" alarms undecided.
        // Sync-family drivers never fire JobQueued, so they have no such total.
        foreach (MetricName::counters() as $metric) {
            if ($metric === MetricName::JobsQueued && $driver instanceof SyncQueue) {
                continue;
            }

            $counts = $this->fold($readings[$metric] ?? [], $keep);
            $metrics[] = Metric::make($metric, array_sum($counts), $dimensions);

            foreach ($counts as $class => $value) {
                $metrics[] = Metric::make($metric, $value, [...$dimensions, 'JobClass' => (string) $class]);
            }
        }

        return [$metrics, $readings];
    }

    /**
     * Keep the registry's first classes and relabel the rest, so the set of
     * published series stays fixed and per-class values still sum to the total.
     *
     * @param array<array-key, int> $counts
     * @param array<string, int>    $keep
     *
     * @return array<string, int>
     */
    private function fold(array $counts, array $keep): array {
        $folded = [];

        foreach ($counts as $class => $value) {
            $label = isset($keep[$class]) ? (string) $class : self::OTHER;
            $folded[$label] = ($folded[$label] ?? 0) + $value;
        }

        return $folded;
    }

    /**
     * A provider that cannot count, or only ever answers zero (the null one,
     * and Laravel Cloud's for its managed queues, which it never stores),
     * publishes nothing rather than a gauge that would keep an alarm green
     * forever. A failing count drops only this gauge.
     */
    /**
     * @param list<string> $queues
     */
    private function failedJobs(string $connection, array $queues): ?int {
        // Cloud wraps the application's provider and answers zero for one that
        // cannot count, so judge the provider it wraps.
        $failer = $this->failer instanceof CloudFailedJobProvider
            ? (fn (): FailedJobProviderInterface => $this->failer)->call($this->failer)
            : $this->failer;

        if (
            ! $failer instanceof CountableFailedJobProvider
            || $failer instanceof NullFailedJobProvider
            || ($this->failer instanceof CloudFailedJobProvider && $connection === 'cloud')
        ) {
            return null;
        }

        try {
            return array_sum(array_map(fn (string $queue): int => (int) $failer->count($connection, $queue), $queues));
        } catch (Throwable $e) {
            $this->handler->report($e);

            return null;
        }
    }

    /**
     * Drivers that cannot inspect jobs have no list, and a payload that
     * cannot be inspected drops only the per-class breakdown.
     *
     * @return Collection<int, mixed>|null
     */
    private function reservedJobs(Queue $driver, string $queue): ?Collection {
        if (! method_exists($driver, 'reservedJobs')) {
            return null;
        }

        try {
            return $driver->reservedJobs($queue);
        } catch (Throwable $e) {
            $this->handler->report($e);

            return null;
        }
    }

    /**
     * @param Collection<int, mixed>|null $jobs
     *
     * @return array<string, int>
     */
    private function byClass(?Collection $jobs): array {
        $counts = [];

        foreach ($jobs ?? [] as $job) {
            if ($job instanceof InspectedJob && is_string($job->name) && $job->name !== '') {
                $name = Counters::label($job->name);
                $counts[$name] = ($counts[$name] ?? 0) + 1;
            }
        }

        return $counts;
    }
}
