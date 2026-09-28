<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Contracts\Cache\Repository;
use Illuminate\Support\Sleep;
use RuntimeException;

/**
 * Throughput counters in a shared cache store. Increments happen as the event
 * fires, so nothing accumulates in process memory, and the steady state costs
 * one round trip per event (two for a class past the registry cap).
 */
final readonly class Counters {
    /**
     * Stores that need a seed expire it; five years is what DynamoDB calls forever.
     */
    private const int TTL = 157_680_000;

    /**
     * How often a live counter re-checks that its class is registered, so one
     * dropped from the registry (eviction, a lost write, a busy lock) is found
     * again. Young counters also re-check at every power of two below it.
     */
    private const int RECHECK = 100;

    /**
     * A class that cannot fit in a full registry has its key parked this far
     * up, so every later increment reads as parked and goes straight to the
     * overflow bucket. Up rather than down, because memcached cannot hold a
     * negative value.
     */
    private const int PARKED = 1_000_000_000_000_000;

    /**
     * A parked key expires, so runaway names do not pile up for long; once
     * gone, a class that is still busy simply parks again.
     */
    private const int PARKED_TTL = 3_600;

    /**
     * Names are stored in the registry the sampler reads, so they are capped.
     */
    public static function label(mixed $name): string {
        return is_string($name) && $name !== '' ? mb_substr($name, 0, 255) : 'unknown';
    }

    public function __construct(
        private Repository $cache,
        private string $prefix = 'queue-monitor',
        private int $maxClasses = 100,
    ) {}

    public function increment(string $connection, string $queue, string $metric, string $class): void {
        $key = $this->key($connection, $queue, $metric, $class);
        $value = $this->bump($key, 1);

        // Only a parked key expires within years, and DynamoDB refuses both
        // the increment and the seed during its expiry second.
        if ($value === null || $value >= self::PARKED) {
            $this->overflow($connection, $queue, $metric, 1);

            return;
        }

        // A counter reads 1 when it is new or was just drained, which is when
        // its class may be missing from the registry the sampler reads.
        if (! self::crossed($value, 1) || $this->register($connection, $queue, $class)) {
            return;
        }

        // The registry is full. Parking moves everything the key holds, this
        // event and any that raced it, in one atomic step. The TTL goes on
        // first, so a failure afterwards cannot leave the key parked for good.
        $this->cache->touch($key, self::PARKED_TTL);
        $moved = ($this->bump($key, self::PARKED) ?? throw self::rejected()) - self::PARKED;

        if ($moved >= self::PARKED) {
            // Another process parked it first and has already moved these.
            $this->cache->decrement($key, self::PARKED);

            return;
        }

        $this->overflow($connection, $queue, $metric, $moved);
    }

    /**
     * @param list<string>      $metrics
     * @param list<string>|null $classes the registry, when the caller already read it
     *
     * @return array<string, array<string, int>> metric => job class => count
     */
    public function read(string $connection, string $queue, array $metrics, ?array $classes = null): array {
        $keys = [];

        foreach ($classes ?? $this->classes($connection, $queue) as $class) {
            foreach ($metrics as $metric) {
                $keys[$this->key($connection, $queue, $metric, $class)] = [$metric, $class];
            }
        }

        $readings = [];

        // DynamoDB's BatchGetItem takes at most 100 keys.
        foreach (array_chunk(array_keys($keys), 100) as $chunk) {
            foreach ($this->cache->many($chunk) as $key => $value) {
                if (is_numeric($value) && (int) $value > 0 && (int) $value < self::PARKED) {
                    [$metric, $class] = $keys[$key];
                    $readings[$metric][$class] = (int) $value;
                }
            }
        }

        return $readings;
    }

    /**
     * Subtract exactly what was read, so increments landing in between survive.
     *
     * @param array<string, array<array-key, int>> $readings
     */
    public function commit(string $connection, string $queue, array $readings): void {
        foreach ($readings as $metric => $classes) {
            foreach ($classes as $class => $value) {
                $key = $this->key($connection, $queue, $metric, (string) $class);
                $left = $this->cache->decrement($key, $value);

                // The key vanished since the read (a flush, an eviction): Redis
                // would leave it negative and swallow that many future events.
                if (is_int($left) && $left < 0) {
                    $this->cache->increment($key, -$left);
                }
            }
        }
    }

    /**
     * The registered job classes, in the order they were first seen.
     *
     * @return list<string>
     */
    public function classes(string $connection, string $queue): array {
        /** @var mixed $stored */
        $stored = $this->cache->get($this->registryKey($connection, $queue));

        return is_array($stored) ? array_values(array_filter($stored, is_string(...))) : [];
    }

    /**
     * Increment, seeding the key on stores that refuse to increment one that
     * does not exist. Redis creates it, so it never reaches the seed. Null
     * when the store refuses both.
     */
    private function bump(string $key, int $by): ?int {
        $value = $this->cache->increment($key, $by);

        if ($value === false) {
            $value = $this->cache->add($key, $by, self::TTL) ? $by : $this->cache->increment($key, $by);
        }

        return is_int($value) ? $value : null;
    }

    private static function rejected(): RuntimeException {
        return new RuntimeException('The queue-monitor counter store rejected an increment.');
    }

    private function overflow(string $connection, string $queue, string $metric, int $by): void {
        $value = $this->bump($this->key($connection, $queue, $metric, Collector::OTHER), $by) ?? throw self::rejected();

        if (self::crossed($value, $by)) {
            $this->register($connection, $queue, Collector::OTHER);
        }
    }

    /**
     * Whether a counter that moved by $by just started or passed a recheck mark.
     */
    private static function crossed(int $value, int $by): bool {
        return $value === $by
            || ($value > 0 && $value < self::RECHECK && ($value & ($value - 1)) === 0)
            || intdiv($value, self::RECHECK) !== intdiv($value - $by, self::RECHECK);
    }

    /**
     * Whether the class may keep counting under its own name: false only when
     * the registry is full.
     */
    private function register(string $connection, string $queue, string $class): bool {
        $classes = $this->classes($connection, $queue);

        if (in_array($class, $classes, true)) {
            return true;
        }

        if ($class !== Collector::OTHER && count($classes) >= $this->maxClasses) {
            return false;
        }

        $store = $this->cache->getStore();

        if (! $store instanceof LockProvider) {
            throw new RuntimeException('The queue-monitor counter store must support locks.');
        }

        $key = $this->registryKey($connection, $queue);
        $lock = $store->lock($key.':lock', 5);

        // The critical section is two round trips, so the wait is short. Past
        // it, the count stays where it is and the next recheck registers it.
        for ($attempt = 1; ! $lock->get(); $attempt++) {
            if ($attempt === 5) {
                return true;
            }

            Sleep::usleep(10_000);
        }

        try {
            $classes = $this->classes($connection, $queue);

            if (in_array($class, $classes, true)) {
                return true;
            }

            if ($class !== Collector::OTHER && count($classes) >= $this->maxClasses) {
                return false;
            }

            $this->cache->forever($key, [...$classes, $class]);

            return true;
        } finally {
            $lock->release();
        }
    }

    private function key(string $connection, string $queue, string $metric, string $class): string {
        return $this->prefix.':v:'.hash('xxh128', implode("\0", [$connection, $queue, $metric, $class]));
    }

    private function registryKey(string $connection, string $queue): string {
        return $this->prefix.':c:'.hash('xxh128', $connection."\0".$queue);
    }
}
