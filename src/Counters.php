<?php

declare(strict_types=1);

namespace CodingDuck\QueueMonitor;

use Illuminate\Cache\Lock;
use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Contracts\Cache\Repository;
use RuntimeException;

final readonly class Counters {
    public const string OTHER = '__other__';

    /**
     * Repository::add() is only atomic with a TTL, so a seed gets one that never matters.
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
     * overflow bucket.
     */
    private const int PARKED = 1_000_000_000_000_000;

    /**
     * A parked key expires, so runaway names do not pile up.
     */
    private const int PARKED_TTL = 120;

    public function __construct(
        private Repository $cache,
        private string $prefix,
        private int $maxClasses = 100,
    ) {}

    public function increment(string $connection, string $queue, string $metric, string $class): void {
        // Without a JobClass dimension only the totals are needed.
        if ($this->maxClasses === 0) {
            $class = self::OTHER;
        }

        $key = $this->key($connection, $queue, $metric, $class);
        $value = $this->bump($key, 1);

        // Null only while a parked key sits in its DynamoDB expiry second.
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

        // Past PARKED, another process parked it first and moved these already.
        if ($moved > 0 && $moved < self::PARKED) {
            $this->overflow($connection, $queue, $metric, $moved);
        }
    }

    /**
     * @param list<string>|null $classes the registry, when the caller already read it
     *
     * @return array<string, array<string, int>> metric => job class => count
     */
    public function read(string $connection, string $queue, ?array $classes = null): array {
        $keys = [];

        foreach ($classes ?? $this->classes($connection, $queue) as $class) {
            foreach (MetricName::COUNTERS as $metric) {
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
        $stored = $this->cache->get($this->queueKey('c', $connection, $queue));

        return is_array($stored) ? array_values(array_filter($stored, is_string(...))) : [];
    }

    /**
     * The lock that keeps two samplers from draining the same counters.
     */
    public function lock(string $connection, string $queue, int $seconds): Lock {
        return $this->newLock($this->queueKey('sample', $connection, $queue), $seconds);
    }

    /**
     * Increment, seeding the key on stores that refuse to increment one that
     * does not exist. Null when the store refuses both.
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
        $value = $this->bump($this->key($connection, $queue, $metric, self::OTHER), $by) ?? throw self::rejected();

        if (self::crossed($value, $by)) {
            $this->register($connection, $queue, self::OTHER);
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

        if ($class !== self::OTHER && count($classes) >= $this->maxClasses) {
            return false;
        }

        $key = $this->queueKey('c', $connection, $queue);
        $lock = $this->newLock($key.':lock', 5);

        // Past a busy lock the count stays where it is and the next recheck registers it.
        if (! $lock->get()) {
            return true;
        }

        try {
            $classes = $this->classes($connection, $queue);

            if (in_array($class, $classes, true)) {
                return true;
            }

            if ($class !== self::OTHER && count($classes) >= $this->maxClasses) {
                return false;
            }

            $this->cache->forever($key, [...$classes, $class]);

            return true;
        } finally {
            $lock->release();
        }
    }

    private function newLock(string $name, int $seconds): Lock {
        $store = $this->cache->getStore();
        $lock = $store instanceof LockProvider ? $store->lock($name, $seconds) : null;

        return $lock instanceof Lock ? $lock : throw new RuntimeException('The queue-monitor counter store must support locks.');
    }

    private function key(string $connection, string $queue, string $metric, string $class): string {
        return $this->prefix.':v:'.hash('xxh128', implode("\0", [$connection, $queue, $metric, $class]));
    }

    private function queueKey(string $kind, string $connection, string $queue): string {
        return $this->prefix.':'.$kind.':'.hash('xxh128', $connection."\0".$queue);
    }
}
