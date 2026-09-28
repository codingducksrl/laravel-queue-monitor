<?php

declare(strict_types=1);

use CodingDuck\QueueMonitor\Metric;
use CodingDuck\QueueMonitor\Sinks\Emf\EmfSink;
use CodingDuck\QueueMonitor\Sinks\Emf\StdoutEmitter;
use CodingDuck\QueueMonitor\Tests\Support\FakeEmitter;
use Illuminate\Support\Carbon;

beforeEach(function (): void {
    Carbon::setTestNow(Carbon::parse('2026-09-21T09:00:00Z'));
    $this->emitter = new FakeEmitter;
    $this->sink = new EmfSink($this->emitter, 'Acme/Queues');
});

afterEach(function (): void {
    Carbon::setTestNow();
});

function queueLevel(): array {
    return ['Connection' => 'redis', 'Queue' => 'default'];
}

function classLevel(string $class): array {
    return ['Connection' => 'redis', 'Queue' => 'default', 'JobClass' => $class];
}

it('emits one document per dimension tuple', function (): void {
    $this->sink->write([
        Metric::make('JobsCompleted', 165, queueLevel()),
        Metric::make('JobsCompleted', 120, classLevel('App\Jobs\SendInvoice')),
        Metric::make('JobsCompleted', 45, classLevel('App\Jobs\SyncContact')),
    ]);

    expect($this->emitter->documents())->toHaveCount(3);
});

it('gives every document exactly one dimension set matching its own tuple', function (): void {
    $this->sink->write([
        Metric::make('JobsCompleted', 165, queueLevel()),
        Metric::make('JobsCompleted', 120, classLevel('App\Jobs\SendInvoice')),
    ]);

    $sets = array_map(
        static fn (array $document): array => $document['_aws']['CloudWatchMetrics'][0]['Dimensions'],
        $this->emitter->documents(),
    );

    expect($sets)->toBe([
        [['Connection', 'Queue']],
        [['Connection', 'Queue', 'JobClass']],
    ]);
});

it('publishes the queue level value exactly once across the batch', function (): void {
    $this->sink->write([
        Metric::make('JobsCompleted', 165, queueLevel()),
        Metric::make('JobsCompleted', 120, classLevel('App\Jobs\SendInvoice')),
        Metric::make('JobsCompleted', 45, classLevel('App\Jobs\SyncContact')),
    ]);

    $published = [];

    foreach ($this->emitter->documents() as $document) {
        foreach ($document['_aws']['CloudWatchMetrics'][0]['Dimensions'] as $set) {
            $tuple = [];

            foreach ($set as $key) {
                $tuple[$key] = $document[$key];
            }

            foreach ($document['_aws']['CloudWatchMetrics'][0]['Metrics'] as $metric) {
                $published[] = $metric['Name'].'|'.json_encode($tuple);
            }
        }
    }

    expect($published)->toHaveCount(count(array_unique($published)));

    $queueSeries = 'JobsCompleted|'.json_encode(queueLevel());

    expect(array_count_values($published)[$queueSeries])->toBe(1);
});

it('packs every metric sharing a tuple into one document', function (): void {
    $this->sink->write([
        Metric::make('JobsPending', 42, queueLevel()),
        Metric::make('JobsDelayed', 7, queueLevel()),
        Metric::make('JobsInProgress', 3, queueLevel()),
    ]);

    $documents = $this->emitter->documents();

    expect($documents)->toHaveCount(1)
        ->and($documents[0]['_aws']['CloudWatchMetrics'][0]['Metrics'])->toHaveCount(3)
        ->and($documents[0]['JobsPending'])->toBe(42);
});

it('stamps the current time in epoch milliseconds', function (): void {
    $this->sink->write([Metric::make('JobsPending', 1, queueLevel())]);

    expect($this->emitter->documents()[0]['_aws']['Timestamp'])->toBe(1789981200000);
});

it('writes nothing for an empty batch', function (): void {
    $this->sink->write([]);

    expect($this->emitter->lines)->toBeEmpty();
});

it('writes one newline terminated line per document to its stream', function (): void {
    $handle = new SplFileObject('php://memory', 'r+');

    (new StdoutEmitter($handle))->emit('{"a":1}');
    (new StdoutEmitter($handle))->emit('{"b":2}');

    $handle->rewind();

    expect($handle->fread(1024))->toBe("{\"a\":1}\n{\"b\":2}\n");
});

it('groups dimensions that are not valid UTF-8 without failing', function (): void {
    $this->sink->write([Metric::make('JobsPending', 1, ['Connection' => 'redis', 'Queue' => "bad\xFF"])]);

    expect($this->emitter->documents()[0]['Queue'])->toStartWith('bad#');
});

it('emits nothing from a batch that cannot be encoded in full', function (): void {
    expect(fn () => $this->sink->write([
        Metric::make('JobsPending', 1, queueLevel()),
        Metric::make('JobsPending', INF, classLevel('App\Jobs\SendInvoice')),
    ]))->toThrow(JsonException::class);

    expect($this->emitter->lines)->toBeEmpty();
});

it('treats a short write as a failure', function (): void {
    $handle = new class('php://memory', 'r+') extends SplFileObject {
        public function fwrite(string $data, mixed $length = null): int|false {
            return parent::fwrite(substr($data, 0, 3));
        }
    };

    expect(fn () => (new StdoutEmitter($handle))->emit('{"a":1}'))->toThrow(RuntimeException::class, 'Unable to write');
});

it('refuses to write into /dev/null', function (): void {
    expect(fn () => (new StdoutEmitter(new SplFileObject('/dev/null', 'wb')))->emit('{"a":1}'))
        ->toThrow(RuntimeException::class, 'appendOutputTo');
})->skipOnWindows();
