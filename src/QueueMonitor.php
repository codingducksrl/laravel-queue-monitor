<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use Illuminate\Contracts\Config\Repository;

class QueueMonitor {
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
        $connections = $this->config->get('queue-monitor.connections');

        if (! is_array($connections)) {
            return [];
        }

        return array_values(array_filter($connections, is_string(...)));
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
}
