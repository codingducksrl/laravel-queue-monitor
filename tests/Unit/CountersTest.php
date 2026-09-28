<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\MetricName;
use CodingDuck\QueueMonitor\Tests\Support\SpyStore;
use Illuminate\Cache\ArrayStore;
use Illuminate\Cache\Repository;

beforeEach(function (): void {
    $this->store = new ArrayStore;
    $this->counters = new Counters(new Repository($this->store), 'qm-test');
});

function drain(Counters $counters, string $queue = 'default'): array {
    $readings = $counters->read('redis', $queue);
    $counters->commit('redis', $queue, $readings);

    return $readings;
}

function counterKey(string $prefix, string $class): string {
    return $prefix.':v:'.hash('xxh128', implode("\0", ['redis', 'default', MetricName::JobsCompleted, $class]));
}

it('accumulates counts per connection, queue, metric and job class', function (): void {
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\SendInvoice');
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\SendInvoice');
    $this->counters->increment('redis', 'default', MetricName::JobsFailed, 'App\Jobs\SyncContact');

    expect($this->counters->read('redis', 'default'))->toBe([
        MetricName::JobsCompleted => ['App\Jobs\SendInvoice' => 2],
        MetricName::JobsFailed => ['App\Jobs\SyncContact' => 1],
    ]);
});

it('keeps counts on separate queues apart', function (): void {
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\SendInvoice');
    $this->counters->increment('redis', 'high', MetricName::JobsCompleted, 'App\Jobs\SendInvoice');

    expect($this->counters->read('redis', 'high'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\SendInvoice' => 1]]);
});

it('keeps names that would collide once joined apart', function (): void {
    $this->counters->increment('a:b', 'c', MetricName::JobsCompleted, 'App\Jobs\A');
    $this->counters->increment('a', 'b:c', MetricName::JobsCompleted, 'App\Jobs\A');
    $this->counters->increment('a', 'b:c', MetricName::JobsCompleted, 'App.Jobs.A');

    expect($this->counters->read('a:b', 'c'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1]])
        ->and($this->counters->read('a', 'b:c'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1, 'App.Jobs.A' => 1]]);
});

it('builds keys every store accepts whatever the job is called', function (): void {
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'Closure (routes/web.php:12) '.str_repeat('x', 300));

    foreach (array_keys((fn (): array => $this->storage)->call($this->store)) as $key) {
        expect(strlen($key))->toBeLessThanOrEqual(250)->and($key)->not->toMatch('/[\s\x00-\x1F]/');
    }
});

it('registers each job class once, in the order first seen', function (): void {
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\SendInvoice');
    $this->counters->increment('redis', 'default', MetricName::JobsFailed, 'App\Jobs\SendInvoice');
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\SyncContact');

    expect($this->counters->classes('redis', 'default'))
        ->toBe(['App\Jobs\SendInvoice', 'App\Jobs\SyncContact']);
});

it('reports no classes for a queue that has seen nothing', function (): void {
    expect($this->counters->classes('redis', 'quiet'))->toBe([]);
});

it('omits counters that are back at zero', function (): void {
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\SendInvoice');

    drain($this->counters);

    expect($this->counters->read('redis', 'default'))->toBe([]);
});

it('keeps increments that land between the read and the commit', function (): void {
    foreach (range(1, 10) as $ignored) {
        $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\SendInvoice');
    }

    $readings = $this->counters->read('redis', 'default');

    // A worker increments while the sampler is publishing.
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\SendInvoice');

    $this->counters->commit('redis', 'default', $readings);

    expect($readings[MetricName::JobsCompleted]['App\Jobs\SendInvoice'])->toBe(10)
        ->and($this->counters->read('redis', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\SendInvoice' => 1]]);
});

it('costs one store call per event between re-checks', function (): void {
    $spy = new SpyStore;

    // A fresh instance per event, the way PHP-FPM sees it. The 1st and 2nd
    // events re-check the registry; the 3rd is past both marks.
    foreach (range(1, 2) as $ignored) {
        (new Counters(new Repository($spy), 'qm'))->increment('redis', 'default', MetricName::JobsQueued, 'App\Jobs\A');
    }

    $spy->calls = [];
    (new Counters(new Repository($spy), 'qm'))->increment('redis', 'default', MetricName::JobsQueued, 'App\Jobs\A');

    expect($spy->calls)->toBe(['increment']);
});

it('costs two store calls per event for a class past the registry cap', function (): void {
    $spy = new SpyStore;
    $counters = new Counters(new Repository($spy), 'qm', 1);

    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\B');
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\B');
    $spy->calls = [];
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\B');

    expect($spy->calls)->toBe(['increment', 'increment'])
        ->and($counters->read('redis', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1, Counters::OTHER => 3]]);
});

it('seeds a missing key with the store\'s own atomic add', function (): void {
    // DynamoDB refuses to increment a key that does not exist.
    $store = new class extends ArrayStore {
        public array $adds = [];

        public function increment($key, $value = 1) {
            return $this->get($key) === null ? false : parent::increment($key, $value);
        }

        public function add($key, $value, $seconds): bool {
            $this->adds[] = $seconds;

            if ($this->get($key) !== null) {
                return false;
            }

            return $this->put($key, $value, $seconds);
        }
    };

    $counters = new Counters(new Repository($store), 'qm');
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    expect($store->adds)->toBe([157_680_000])
        ->and($counters->read('redis', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 2]]);
});

it('increments a key another process seeded first', function (): void {
    $store = new class extends ArrayStore {
        public function increment($key, $value = 1) {
            return $this->get($key) === null ? false : parent::increment($key, $value);
        }

        public function add($key, $value, $seconds): bool {
            $this->put($key, $value, $seconds);

            return false;
        }
    };

    $counters = new Counters(new Repository($store), 'qm');
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    expect($counters->read('redis', 'default'))->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 2]]);
});

it('registers the class again once its registry entry is lost', function (): void {
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $this->store->flush();

    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    expect($this->counters->read('redis', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1]]);
});

it('finds a counter that kept running while its class was dropped from the registry', function (): void {
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $this->store->forget('qm-test:c:'.hash('xxh128', "redis\0default"));

    foreach (range(2, 100) as $ignored) {
        $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    }

    expect($this->counters->read('redis', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 100]]);
});

it('finds a class dropped from the registry at its next hundredth event', function (): void {
    foreach (range(1, 64) as $ignored) {
        $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    }

    $this->store->forget('qm-test:c:'.hash('xxh128', "redis\0default"));

    foreach (range(65, 99) as $ignored) {
        $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    }

    expect($this->counters->classes('redis', 'default'))->toBe([]);

    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    expect($this->counters->classes('redis', 'default'))->toBe(['App\Jobs\A']);
});

it('finds a class dropped from the registry at its next power of two', function (): void {
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    $this->store->forget('qm-test:c:'.hash('xxh128', "redis\0default"));

    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    expect($this->counters->classes('redis', 'default'))->toBe(['App\Jobs\A']);
});

it('re-reads the registry under its lock so a concurrent registration survives', function (): void {
    $store = new class extends ArrayStore {
        public ?Closure $onLock = null;

        public function lock($name, $seconds = 0, $owner = null) {
            if ($hook = $this->onLock) {
                $this->onLock = null;
                $hook();
            }

            return parent::lock($name, $seconds, $owner);
        }
    };
    $counters = new Counters(new Repository($store), 'qm');
    $store->onLock = fn () => (new Counters(new Repository($store), 'qm'))->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\B');

    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    expect($counters->classes('redis', 'default'))->toBe(['App\Jobs\B', 'App\Jobs\A']);
});

it('keeps counting while another process holds the registry lock', function (): void {
    $this->store->lock('qm-test:c:'.hash('xxh128', "redis\0default").':lock', 60)->get();

    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    expect($this->counters->classes('redis', 'default'))->toBe([]);

    (fn () => $this->locks = [])->call($this->store);

    foreach (range(2, 100) as $ignored) {
        $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    }

    expect($this->counters->read('redis', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 100]]);
});

it('counts classes past the registry cap in the overflow bucket', function (): void {
    $counters = new Counters(new Repository($this->store), 'qm-cap', 2);

    foreach (['A', 'B', 'C', 'D', 'C'] as $class) {
        $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\\'.$class);
    }

    $entry = (fn (): array => $this->storage[counterKey('qm-cap', 'App\Jobs\C')])->call($this->store);

    expect($counters->classes('redis', 'default'))->toBe(['App\Jobs\A', 'App\Jobs\B', Counters::OTHER])
        ->and($counters->read('redis', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1, 'App\Jobs\B' => 1, Counters::OTHER => 3]])
        ->and($entry['value'])->toBeGreaterThanOrEqual(1_000_000_000_000_000)
        ->and($entry['expiresAt'])->not->toBe(0);
});

it('moves the increments that raced the parking of a class past the cap', function (): void {
    $counters = new Counters(new Repository($this->store), 'qm-race', 1);
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    // Another process counted B's first event and has not parked it yet.
    $this->store->increment(counterKey('qm-race', 'App\Jobs\B'), 1);
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\B');

    expect($counters->read('redis', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1, Counters::OTHER => 2]]);
});

it('never reads a parked key as a count', function (): void {
    $this->store->forever('qm-test:c:'.hash('xxh128', "redis\0default"), ['App\Jobs\A', 'App\Jobs\B']);
    $this->store->increment(counterKey('qm-test', 'App\Jobs\A'), 3);
    $this->store->increment(counterKey('qm-test', 'App\Jobs\B'), 1_000_000_000_000_003);

    expect($this->counters->read('redis', 'default'))->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 3]]);
});

it('moves nothing twice when two processes park the same class', function (): void {
    $store = new class extends ArrayStore {
        public ?Closure $onTouch = null;

        public function touch($key, $seconds) {
            if ($hook = $this->onTouch) {
                $this->onTouch = null;
                $hook();
            }

            return parent::touch($key, $seconds);
        }
    };
    $counters = new Counters(new Repository($store), 'qm', 1);
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    // B's second event lands, re-checks and parks while the first is parking.
    $store->onTouch = fn () => (new Counters(new Repository($store), 'qm', 1))->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\B');
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\B');

    expect($counters->read('redis', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1, Counters::OTHER => 2]]);
});

it('counts into the overflow bucket when a store refuses a parked key for a second', function (): void {
    $store = new class(counterKey('qm-edge', 'App\Jobs\B')) extends ArrayStore {
        public function __construct(private string $refusing) {
            parent::__construct();
        }

        public function increment($key, $value = 1) {
            return $key === $this->refusing ? false : parent::increment($key, $value);
        }

        public function add($key, $value, $seconds): bool {
            return $key === $this->refusing ? false : $this->put($key, $value, $seconds);
        }
    };
    $counters = new Counters(new Repository($store), 'qm-edge');

    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\B');

    expect($counters->read('redis', 'default'))
        ->toBe([MetricName::JobsCompleted => [Counters::OTHER => 1]]);
});

it('counts only the totals when the class dimension is off', function (): void {
    $spy = new SpyStore;
    $counters = new Counters(new Repository($spy), 'qm', 0);

    foreach (['A', 'B', 'C', 'D'] as $class) {
        $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\\'.$class);
    }

    $spy->calls = [];
    $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\E');

    expect($spy->calls)->toBe(['increment'])
        ->and($counters->classes('redis', 'default'))->toBe([Counters::OTHER])
        ->and($counters->read('redis', 'default'))->toBe([MetricName::JobsCompleted => [Counters::OTHER => 5]]);
});

it('drains a class whose name is a number', function (): void {
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, '42');

    expect(drain($this->counters))->toBe([MetricName::JobsCompleted => [42 => 1]])
        ->and($this->counters->read('redis', 'default'))->toBe([]);
});

it('reads counters in batches every store accepts', function (): void {
    $spy = new class extends ArrayStore {
        public array $batches = [];

        public function many(array $keys) {
            $this->batches[] = count($keys);

            return parent::many($keys);
        }
    };
    $counters = new Counters(new Repository($spy), 'qm');

    foreach (range(1, 40) as $i) {
        $counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\Job'.$i);
    }

    expect($counters->read('redis', 'default'))->toHaveKey(MetricName::JobsCompleted)
        ->and($spy->batches)->toBe([100, 20]);
});

it('does not leave a counter negative when its key vanished before the commit', function (): void {
    foreach (range(1, 5) as $ignored) {
        $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');
    }

    $readings = $this->counters->read('redis', 'default');
    $this->store->flush();
    $this->counters->commit('redis', 'default', $readings);

    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\A');

    expect($this->counters->read('redis', 'default'))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\A' => 1]]);
});
