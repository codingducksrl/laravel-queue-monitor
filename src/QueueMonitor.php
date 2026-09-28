<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Container\Container;
use Illuminate\Contracts\Queue\Factory;
use Illuminate\Foundation\Cloud\Queue as CloudQueue;
use Illuminate\Queue\QueueRoutes;
use Illuminate\Queue\SqsQueue;
use Illuminate\Queue\SyncQueue;
use InvalidArgumentException;

class QueueMonitor {
    /** @var list<array{0: string, 1: string}>|null */
    private ?array $pairs = null;

    /** @var array<string, array<string, string>> connection => reported queue name => monitored queue */
    private array $aliases = [];

    public function __construct(
        private readonly Repository $config,
        private readonly Container $app,
    ) {}

    /**
     * Whether monitoring is switched on at all.
     */
    public function enabled(): bool {
        return filter_var($this->config->get('queue-monitor.enabled'), FILTER_VALIDATE_BOOL);
    }

    /**
     * The connection and queue pairs that are counted and sampled.
     *
     * @return list<array{0: string, 1: string}>
     */
    public function sampledQueues(): array {
        return $this->pairs ??= $this->configuredPairs();
    }

    /**
     * The monitored queue a job reported under the given name counts towards,
     * or null when that queue is not monitored. Drivers do not agree on how
     * they name a queue: SQS workers report the full queue URL, and forwarded
     * queues are reported under their target, so every spelling maps back to
     * the configured name.
     */
    public function monitoredQueue(string $connection, string $queue): ?string {
        $this->aliases[$connection] ??= $this->aliasesFor($connection);

        return $this->aliases[$connection][$this->forwarded($connection, $queue)] ?? null;
    }

    /**
     * The name a worker reports a monitored queue under, which is also what
     * the failed job store records.
     */
    public function physicalQueue(string $connection, string $queue): string {
        $driver = $this->app->make(Factory::class)->connection($connection);

        if ($driver instanceof SqsQueue) {
            return $driver->getQueue($queue);
        }

        // Sync, deferred and background jobs all report the queue as "sync",
        // so they count towards the connection's first monitored queue.
        if ($driver instanceof SyncQueue) {
            $first = array_values(array_filter($this->sampledQueues(), fn (array $pair): bool => $pair[0] === $connection))[0][1] ?? $queue;

            return $queue === $first ? 'sync' : $queue;
        }

        // Laravel Cloud decorates an SQS queue and forwards the call to it.
        $url = $driver instanceof CloudQueue ? $driver->__call('getQueue', [$queue]) : null;

        return is_string($url) ? $url : $this->forwarded($connection, $queue);
    }

    public function defaultConnection(): string {
        $connection = $this->config->get('queue.default');

        return is_string($connection) && $connection !== '' ? $connection : 'sync';
    }

    public function defaultQueue(string $connection): string {
        $queue = $this->config->get("queue.connections.{$connection}.queue");

        return is_string($queue) && $queue !== '' ? $queue : 'default';
    }

    public function sink(): string {
        $sink = $this->config->get('queue-monitor.sink');

        return is_string($sink) && $sink !== '' ? $sink : 'null';
    }

    public function counterStore(): ?string {
        $store = $this->config->get('queue-monitor.counters.store');

        return is_string($store) && $store !== '' ? $store : null;
    }

    public function counterPrefix(): string {
        $prefix = $this->config->get('queue-monitor.counters.prefix');

        return is_string($prefix) && $prefix !== '' ? $prefix : 'queue-monitor';
    }

    public function maxJobClasses(): int {
        $max = $this->config->get('queue-monitor.max_job_classes');

        return is_int($max) && $max > 0 ? $max : 25;
    }

    /**
     * @return list<array{0: string, 1: string}>
     */
    private function configuredPairs(): array {
        $configured = $this->config->get('queue-monitor.queues');

        if (! is_array($configured) || $configured === []) {
            $connection = $this->innerConnection($this->defaultConnection());

            return [[$connection, $this->defaultQueue($connection)]];
        }

        $pairs = [];

        foreach ($configured as $connection => $queues) {
            if (! is_string($connection) || $connection === '') {
                throw new InvalidArgumentException(
                    'queue-monitor.queues maps connection names to their queues, e.g. [\'redis\' => [\'default\']].'
                );
            }

            $connection = $this->innerConnection($connection);
            $queues = array_filter((array) $queues, fn (mixed $queue): bool => is_string($queue) && $queue !== '');

            foreach ($queues === [] ? [$this->defaultQueue($connection)] : $queues as $queue) {
                $pairs[] = [$connection, $queue];
            }
        }

        return array_values(array_unique($pairs, SORT_REGULAR));
    }

    /**
     * @return array<string, string>
     */
    private function aliasesFor(string $connection): array {
        $aliases = [];

        foreach ($this->sampledQueues() as [$monitored, $queue]) {
            if ($monitored !== $connection) {
                continue;
            }

            $aliases[$this->forwarded($connection, $queue)] = $queue;
            $aliases[$this->physicalQueue($connection, $queue)] = $queue;
        }

        return $aliases;
    }

    /**
     * A failover connection pushes and pops through its first connection, and
     * jobs report that one, so it is monitored under that name.
     */
    public function innerConnection(string $connection): string {
        $inner = $this->config->get("queue.connections.{$connection}.connections.0");

        return $this->config->get("queue.connections.{$connection}.driver") === 'failover' && is_string($inner) && $inner !== ''
            ? $inner
            : $connection;
    }

    private function forwarded(string $connection, string $queue): string {
        /** @var QueueRoutes $routes */
        $routes = $this->app->make('queue.routes');

        return $routes->forwardedQueue($queue, $connection);
    }
}
