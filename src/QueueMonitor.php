<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use Illuminate\Contracts\Config\Repository;

class QueueMonitor {
    /** @var list<string>|null */
    private ?array $connections = null;

    public function __construct(private readonly Repository $config) {}

    /**
     * Whether monitoring is switched on at all.
     */
    public function enabled(): bool {
        return $this->config->get('queue-monitor.enabled') === true;
    }

    /**
     * The queue connections monitoring applies to. An empty list means every
     * connection is monitored.
     *
     * @return list<string>
     */
    public function connections(): array {
        if ($this->connections !== null) {
            return $this->connections;
        }

        $connections = $this->config->get('queue-monitor.connections');

        if (! is_array($connections)) {
            return $this->connections = [];
        }

        return $this->connections = array_values(array_filter($connections, is_string(...)));
    }

    /**
     * Whether a given connection is covered by the current configuration.
     */
    public function monitors(string $connection): bool {
        if (! $this->enabled()) {
            return false;
        }

        $connections = $this->connections();

        return $connections === [] || in_array($connection, $connections, true);
    }

    /**
     * The connection and queue pairs the sampler reads.
     *
     * @return list<array{0: string, 1: string}>
     */
    public function sampledQueues(): array {
        $configured = $this->config->get('queue-monitor.queues');

        if (! is_array($configured) || $configured === []) {
            return [[$this->defaultConnection(), $this->defaultQueue($this->defaultConnection())]];
        }

        $pairs = [];

        foreach ($configured as $connection => $queues) {
            $connection = (string) $connection;

            if (! is_array($queues) || $queues === []) {
                $pairs[] = [$connection, $this->defaultQueue($connection)];

                continue;
            }

            foreach (array_filter($queues, is_string(...)) as $queue) {
                $pairs[] = [$connection, $queue];
            }
        }

        return $pairs;
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
}
