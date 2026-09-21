<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Counters;
use CodingDuck\QueueMonitor\MetricName;
use Illuminate\Cache\ArrayStore;
use Illuminate\Cache\Repository;

beforeEach(function (): void {
    $this->counters = new Counters(new Repository(new ArrayStore), 'qm-test');
});

it('accumulates counts per connection, queue, metric and job class', function (): void {
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\SendInvoice');
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\SendInvoice');
    $this->counters->increment('redis', 'default', MetricName::JobsFailed, 'App\Jobs\SyncContact');

    expect($this->counters->read('redis', 'default', MetricName::counters()))->toBe([
        MetricName::JobsCompleted => ['App\Jobs\SendInvoice' => 2],
        MetricName::JobsFailed => ['App\Jobs\SyncContact' => 1],
    ]);
});

it('keeps counts on separate queues apart', function (): void {
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\SendInvoice');
    $this->counters->increment('redis', 'high', MetricName::JobsCompleted, 'App\Jobs\SendInvoice');

    expect($this->counters->read('redis', 'high', MetricName::counters()))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\SendInvoice' => 1]]);
});

it('registers each job class once', function (): void {
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

    $readings = $this->counters->read('redis', 'default', MetricName::counters());
    $this->counters->commit('redis', 'default', $readings);

    expect($this->counters->read('redis', 'default', MetricName::counters()))->toBe([]);
});

it('keeps increments that land between the read and the commit', function (): void {
    foreach (range(1, 10) as $ignored) {
        $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\SendInvoice');
    }

    $readings = $this->counters->read('redis', 'default', MetricName::counters());

    // A worker increments while the sampler is publishing.
    $this->counters->increment('redis', 'default', MetricName::JobsCompleted, 'App\Jobs\SendInvoice');

    $this->counters->commit('redis', 'default', $readings);

    expect($readings[MetricName::JobsCompleted]['App\Jobs\SendInvoice'])->toBe(10)
        ->and($this->counters->read('redis', 'default', MetricName::counters()))
        ->toBe([MetricName::JobsCompleted => ['App\Jobs\SendInvoice' => 1]]);
});
