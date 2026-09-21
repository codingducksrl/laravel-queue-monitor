<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Contracts\Cache\Repository;
use RuntimeException;

/**
 * Throughput counters in a shared cache store. Increments happen as the event
 * fires, so nothing accumulates in process memory.
 */
final class Counters {
    /** @var array<string, true> */
    private array $registered = [];

    /** @var array<string, true> */
    private array $seeded = [];

    public function __construct(
        private readonly Repository $cache,
        private readonly string $prefix = 'queue-monitor',
    ) {}

    public function increment(string $connection, string $queue, string $metric, string $class): void {
        $key = $this->key($connection, $queue, $metric, $class);

        if (! isset($this->seeded[$key])) {
            $this->register($connection, $queue, $class);

            // Database and memcached stores refuse to increment a key that does
            // not exist yet. add() is atomic, so seeding cannot cost a count.
            $this->cache->add($key, 0);

            $this->seeded[$key] = true;
        }

        if ($this->cache->increment($key) === false) {
            throw new RuntimeException('The queue-monitor counter store rejected an increment.');
        }
    }

    /**
     * @param list<string> $metrics
     *
     * @return array<string, array<string, int>> metric => job class => count
     */
    public function read(string $connection, string $queue, array $metrics): array {
        $readings = [];

        foreach ($this->classes($connection, $queue) as $class) {
            foreach ($metrics as $metric) {
                $value = $this->cache->get($this->key($connection, $queue, $metric, $class), 0);

                if (is_numeric($value) && (int) $value > 0) {
                    $readings[$metric][$class] = (int) $value;
                }
            }
        }

        return $readings;
    }

    /**
     * Subtract exactly what was read, so increments landing in between survive.
     *
     * @param array<string, array<string, int>> $readings
     */
    public function commit(string $connection, string $queue, array $readings): void {
        foreach ($readings as $metric => $classes) {
            foreach ($classes as $class => $value) {
                $this->cache->decrement($this->key($connection, $queue, $metric, $class), $value);
            }
        }
    }

    /**
     * @return list<string>
     */
    public function classes(string $connection, string $queue): array {
        /** @var mixed $stored */
        $stored = $this->cache->get($this->registryKey($connection, $queue), []);

        if (! is_array($stored)) {
            return [];
        }

        return array_values(array_filter($stored, is_string(...)));
    }

    private function register(string $connection, string $queue, string $class): void {
        $memo = $connection.'|'.$queue.'|'.$class;

        if (isset($this->registered[$memo])) {
            return;
        }

        if (! in_array($class, $this->classes($connection, $queue), true)) {
            $this->append($connection, $queue, $class);
        }

        $this->registered[$memo] = true;
    }

    private function append(string $connection, string $queue, string $class): void {
        $key = $this->registryKey($connection, $queue);
        $store = $this->cache->getStore();

        if (! $store instanceof LockProvider) {
            $this->cache->forever($key, [...$this->classes($connection, $queue), $class]);

            return;
        }

        $store->lock($key.':lock', 10)->get(function () use ($connection, $queue, $class, $key): void {
            $classes = $this->classes($connection, $queue);

            if (! in_array($class, $classes, true)) {
                $this->cache->forever($key, [...$classes, $class]);
            }
        });
    }

    private function key(string $connection, string $queue, string $metric, string $class): string {
        return $this->prefix.':v:'.$this->slug($connection.':'.$queue.':'.$metric.':'.$class);
    }

    private function registryKey(string $connection, string $queue): string {
        return $this->prefix.':c:'.$this->slug($connection.':'.$queue);
    }

    private function slug(string $value): string {
        return str_replace('\\', '.', $value);
    }
}
